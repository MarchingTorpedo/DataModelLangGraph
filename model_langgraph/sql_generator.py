from typing import Dict, List
import pandas as pd

SQL_TYPE_MAP = {
    'INTEGER': 'INTEGER',
    'FLOAT': 'FLOAT',
    'BOOLEAN': 'BOOLEAN',
    'TIMESTAMP': 'TIMESTAMP',
    'TEXT': 'TEXT'
}

def pandas_type_to_sql(dtype: str) -> str:
    return SQL_TYPE_MAP.get(dtype, 'TEXT')

def generate_create_statements(
    tables: Dict[str, pd.DataFrame],
    analysis: Dict,
    table_names: list | None = None,
    drop_if_exists: bool = False,
    if_not_exists: bool = False,
    schema_name: str | None = None,
    ref_layer_name: str | None = None,
    layer_schema_map: Dict[str, str] | None = None,
    table_schema_map: Dict[str, str] | None = None,
) -> str:
    """Generate CREATE TABLE statements for given tables.

    Parameters:
    - tables: mapping of table_name -> DataFrame
    - analysis: analyzer output with pks/fks/types/descriptions
    - table_names: optional list of table names to include (defaults to all)
    - drop_if_exists: if True, prepend DROP TABLE IF EXISTS for each table
    - if_not_exists: if True, emit CREATE TABLE IF NOT EXISTS
    """
    pks = analysis.get('pks', {})
    fks = analysis.get('fks', {})
    types = analysis.get('types', {})
    descriptions = analysis.get('descriptions', {})

    selected = table_names if table_names is not None else list(tables.keys())

    stmts: list[str] = []
    # If a schema_name is provided, emit a CREATE SCHEMA header once
    if schema_name:
        stmts.append(f'CREATE SCHEMA IF NOT EXISTS {schema_name};')
    def qname(name: str, schema: str | None = None) -> str:
        # allow per-table schema override via table_schema_map
        if table_schema_map and name in table_schema_map:
            schema = table_schema_map[name]
        if schema:
            return f'{schema}."{name}"'
        return f'"{name}"'

    for t in selected:
        if t not in tables:
            continue
        df = tables[t]
        cols = []
        for col in df.columns:
            dtype = types.get(t, {}).get(col, 'TEXT')
            sql_type = pandas_type_to_sql(dtype)
            col_def = f'    "{col}" {sql_type}'
            if pks.get(t) == col:
                col_def += ' PRIMARY KEY'
            cols.append(col_def)

        create_kw = 'CREATE TABLE'
        if if_not_exists:
            create_kw = 'CREATE TABLE IF NOT EXISTS'

        table_ident = qname(t, schema_name)
        table_stmt = f'{create_kw} {table_ident} (\n' + ',\n'.join(cols) + '\n);'

        if drop_if_exists:
            stmts.append(f'DROP TABLE IF EXISTS {table_ident};')
        stmts.append(table_stmt)

    # separate ALTER TABLE ADD CONSTRAINT statements for FKs (only include if both tables present)
    for (tbl, col), (ref_tbl, ref_col) in fks.items():
        # Only emit FK for tables we are creating in this statement (tbl in selected)
        if tbl not in selected:
            continue

        # Source table identifier uses per-table mapping or the provided schema_name
        src_ident = qname(tbl, schema_name)

        # Determine the referenced table identifier: prefer per-table mapping, then selected schema, then layer mapping
        if ref_tbl in selected:
            ref_ident = qname(ref_tbl, schema_name)
        elif table_schema_map and ref_tbl in table_schema_map:
            ref_ident = qname(ref_tbl, table_schema_map[ref_tbl])
        elif ref_layer_name and layer_schema_map and ref_layer_name in layer_schema_map:
            ref_schema = layer_schema_map[ref_layer_name]
            ref_ident = qname(ref_tbl, ref_schema)
        else:
            # skip FK if referenced table not present and no mapping provided
            continue

        fk_stmt = f'ALTER TABLE {src_ident} ADD FOREIGN KEY ("{col}") REFERENCES {ref_ident} ("{ref_col}");'
        stmts.append(fk_stmt)

    return '\n\n'.join(stmts)

def save_sql(sql_text: str, out_path: str):
    with open(out_path, 'w', encoding='utf8') as fh:
        fh.write(sql_text)

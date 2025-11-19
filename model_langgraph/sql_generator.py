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

def generate_create_statements(tables: Dict[str, pd.DataFrame], analysis: Dict, table_names: list | None = None, drop_if_exists: bool = False, if_not_exists: bool = False, ref_layer_prefix: str | None = None) -> str:
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

        table_stmt = f'{create_kw} "{t}" (\n' + ',\n'.join(cols) + '\n);'

        if drop_if_exists:
            stmts.append(f'DROP TABLE IF EXISTS "{t}";')
        stmts.append(table_stmt)

    # separate ALTER TABLE ADD CONSTRAINT statements for FKs (only include if both tables present)
    for (tbl, col), (ref_tbl, ref_col) in fks.items():
        # Only emit FK for tables we are creating in this statement (tbl in selected)
        if tbl not in selected:
            continue
        # Determine how to reference the referenced table: if ref_tbl is included in this
        # selected set, reference it directly. Otherwise, if a ref_layer_prefix is provided,
        # prefix the referenced table name with it (e.g., 'bronze.customers' or 'silver.orders').
        if ref_tbl in selected:
            ref_name = f'"{ref_tbl}"'
        elif ref_layer_prefix:
            # allow dot-qualified identifier, do not quote the dot
            ref_name = f'{ref_layer_prefix}."{ref_tbl}"'
        else:
            # skip FK if referenced table not present and no prefix provided
            continue

        fk_stmt = f'ALTER TABLE "{tbl}" ADD FOREIGN KEY ("{col}") REFERENCES {ref_name} ("{ref_col}");'
        stmts.append(fk_stmt)

    return '\n\n'.join(stmts)

def save_sql(sql_text: str, out_path: str):
    with open(out_path, 'w', encoding='utf8') as fh:
        fh.write(sql_text)

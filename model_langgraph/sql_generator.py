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

def generate_create_statements(tables: Dict[str, pd.DataFrame], analysis: Dict) -> str:
    pks = analysis.get('pks', {})
    fks = analysis.get('fks', {})
    types = analysis.get('types', {})
    descriptions = analysis.get('descriptions', {})

    stmts = []
    for t, df in tables.items():
        cols = []
        for col in df.columns:
            dtype = types.get(t, {}).get(col, 'TEXT')
            sql_type = pandas_type_to_sql(dtype)
            col_def = f'    "{col}" {sql_type}'
            if pks.get(t) == col:
                col_def += ' PRIMARY KEY'
            if descriptions.get(t, {}).get(col):
                # attach as comment where supported
                pass
            cols.append(col_def)
        # add FK constraints
        table_stmt = f'CREATE TABLE "{t}" (\n' + ',\n'.join(cols) + '\n);'
        stmts.append(table_stmt)

    # separate ALTER TABLE ADD CONSTRAINT statements for FKs
    for (tbl, col), (ref_tbl, ref_col) in fks.items():
        fk_stmt = f'ALTER TABLE "{tbl}" ADD FOREIGN KEY ("{col}") REFERENCES "{ref_tbl}" ("{ref_col}");'
        stmts.append(fk_stmt)

    return '\n\n'.join(stmts)

def save_sql(sql_text: str, out_path: str):
    with open(out_path, 'w', encoding='utf8') as fh:
        fh.write(sql_text)

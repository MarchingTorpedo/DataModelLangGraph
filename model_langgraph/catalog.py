import json
from typing import Dict
import pandas as pd

def build_catalog(tables: Dict[str, pd.DataFrame]) -> Dict:
    catalog = {}
    for t, df in tables.items():
        catalog[t] = {
            'rows': int(len(df)),
            'columns': {}
        }
        for col in df.columns:
            s = df[col]
            catalog[t]['columns'][col] = {
                'dtype': str(s.dtype),
                'unique': int(s.nunique(dropna=True)),
                'nulls': int(s.isna().sum()),
                'null_pct': float(s.isna().mean()),
                'sample_values': s.dropna().astype(str).head(5).tolist()
            }
    return catalog

def save_catalog(catalog: Dict, out_path: str):
    with open(out_path, 'w', encoding='utf8') as fh:
        json.dump(catalog, fh, indent=2)

def convert_to_star_schema(tables: Dict[str, pd.DataFrame], analysis: Dict) -> Dict:
    # heuristic: choose largest table by rows as fact table
    fact_table = max(tables.items(), key=lambda kv: len(kv[1]))[0]
    dims = [t for t in tables.keys() if t != fact_table]
    star = {
        'fact': fact_table,
        'dimensions': dims,
        'joins': {}
    }
    # attempt to find join cols via FK analysis
    for (tbl, col), (ref_tbl, ref_col) in analysis.get('fks', {}).items():
        star['joins'][f'{tbl}.{col}'] = f'{ref_tbl}.{ref_col}'
    return star

import os
import pandas as pd
from typing import Dict, Tuple, Any
import numpy as np
import json

try:
    from transformers import pipeline
except Exception:
    pipeline = None

def infer_column_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series.dropna()):
        return 'INTEGER'
    if pd.api.types.is_float_dtype(series.dropna()):
        return 'FLOAT'
    if pd.api.types.is_bool_dtype(series.dropna()):
        return 'BOOLEAN'
    if pd.api.types.is_datetime64_any_dtype(series.dropna()):
        return 'TIMESTAMP'
    return 'TEXT'

def detect_primary_keys(tables: Dict[str, pd.DataFrame]) -> Dict[str, str]:
    pks = {}
    for name, df in tables.items():
        candidates = []
        for col in df.columns:
            s = df[col]
            unique_frac = s.dropna().nunique() / max(1, len(s))
            if col.lower() == 'id' or col.lower().endswith('_id'):
                candidates.append((col, unique_frac))
            if unique_frac > 0.9 and s.dropna().dtype != object:
                candidates.append((col, unique_frac))
        if candidates:
            # pick most unique
            candidates.sort(key=lambda x: -x[1])
            pks[name] = candidates[0][0]
        else:
            # fallback: no PK
            pks[name] = None
    return pks

def detect_foreign_keys(tables: Dict[str, pd.DataFrame], pks: Dict[str, str], min_overlap: float = 0.7) -> Dict[Tuple[str, str], Tuple[str, str]]:
    # returns mapping ((table, col) -> (ref_table, ref_col))
    fks = {}
    # build value -> table map for PKs
    pk_values = {}
    for t, pk in pks.items():
        if pk and pk in tables[t].columns:
            vals = tables[t][pk].dropna().unique()
            pk_values[t] = set(vals)

    for t, df in tables.items():
        for col in df.columns:
            # skip if col is PK of self
            if pks.get(t) == col:
                continue
            vals = set(df[col].dropna().unique())
            if not vals:
                continue
            # infer types for additional check
            try:
                col_type = infer_column_type(df[col])
            except Exception:
                col_type = None

            for ref_table, ref_vals in pk_values.items():
                if ref_table == t:
                    continue
                ref_pk = pks.get(ref_table)
                if not ref_pk:
                    continue
                # heuristic: if >min_overlap of non-null unique values appear in ref pk values
                common = vals.intersection(ref_vals)
                if not common:
                    continue
                overlap = len(common) / max(1, len(vals))

                # basic type check: require same inferred type or both numeric
                try:
                    ref_series = tables[ref_table][ref_pk]
                    ref_type = infer_column_type(ref_series)
                except Exception:
                    ref_type = None

                numeric_types = {'INTEGER', 'FLOAT'}
                types_compatible = False
                if col_type and ref_type:
                    if col_type == ref_type:
                        types_compatible = True
                    elif col_type in numeric_types and ref_type in numeric_types:
                        types_compatible = True

                # lower threshold if column name looks like a foreign key
                threshold = min_overlap
                low_col = col.lower()
                if low_col.endswith('_id') or ref_table.lower() in low_col or low_col == ref_pk.lower():
                    threshold = 0.5

                # require at least a minimal number of common values to avoid single-match noise
                min_common = max(2, int(0.05 * len(vals)))

                if overlap >= threshold and len(common) >= min_common and types_compatible:
                    fks[(t, col)] = (ref_table, ref_pk)
                    break
    return fks

def generate_column_descriptions(tables: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, str]]:
    desc = {}
    use_llm = False
    if pipeline is not None and os.getenv('LOCAL_LLM_MODEL'):
        try:
            gen = pipeline('text-generation', model=os.getenv('LOCAL_LLM_MODEL'))
            use_llm = True
        except Exception:
            use_llm = False

    for t, df in tables.items():
        desc[t] = {}
        for col in df.columns:
            sample = df[col].dropna().astype(str).head(5).tolist()
            simple = f"Column `{col}` of table `{t}`. Type guess: {infer_column_type(df[col])}. Samples: {sample}"
            if use_llm:
                prompt = f"Provide a concise, single-sentence description for this column: {simple}"
                try:
                    out = gen(prompt, max_length=128, do_sample=False)
                    text = out[0]['generated_text'] if isinstance(out, list) else str(out)
                    desc_text = text.strip().split('\n')[0]
                except Exception:
                    desc_text = simple
            else:
                # heuristic descriptions
                low = col.lower()
                if low.endswith('_id') or low == 'id':
                    desc_text = 'Identifier linking to another entity.'
                elif 'date' in low or 'time' in low:
                    desc_text = 'Date or timestamp value.'
                elif any(x in low for x in ['name', 'title', 'desc', 'description']):
                    desc_text = 'Textual descriptive field.'
                elif any(x in low for x in ['qty', 'count', 'number', 'amount', 'price']):
                    desc_text = 'Numeric measure.'
                else:
                    desc_text = simple
            desc[t][col] = desc_text
    return desc

def analyze(tables: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    pks = detect_primary_keys(tables)
    fks = detect_foreign_keys(tables, pks)
    descriptions = generate_column_descriptions(tables)
    types = {t: {c: infer_column_type(df[c]) for c in df.columns} for t, df in tables.items()}
    return {
        'pks': pks,
        'fks': fks,
        'descriptions': descriptions,
        'types': types,
    }


def build_langgraph_model(tables: Dict[str, pd.DataFrame], analysis: Dict) -> Dict:
    """Build a JSON-serializable LangGraph-style model dictionary.

    This creates a structure with tables, columns, types, PKs, and FKs that can be
    consumed by an MCP server or later translated into LangGraph SDK objects.
    """
    pks = analysis.get('pks', {})
    fks = analysis.get('fks', {})
    types = analysis.get('types', {})
    descriptions = analysis.get('descriptions', {})

    model = {
        'tables': {},
        'foreign_keys': []
    }

    for t, df in tables.items():
        model['tables'][t] = {
            'primary_key': pks.get(t),
            'columns': {}
        }
        for col in df.columns:
            model['tables'][t]['columns'][col] = {
                'type': types.get(t, {}).get(col, 'TEXT'),
                'description': descriptions.get(t, {}).get(col),
                'is_primary': (pks.get(t) == col)
            }

    for (tbl, col), (ref_tbl, ref_col) in fks.items():
        model['foreign_keys'].append({
            'table': tbl,
            'column': col,
            'ref_table': ref_tbl,
            'ref_column': ref_col
        })

    # Include a small metadata section
    model['_meta'] = {
        'generated_by': 'ModelLangGraph analyzer',
        'langgraph_compatible': True
    }

    return model


def save_langgraph_model_json(model: Dict, out_path: str):
    import json
    with open(out_path, 'w', encoding='utf8') as fh:
        json.dump(model, fh, indent=2)

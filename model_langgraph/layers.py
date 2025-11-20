import os
from typing import Dict, Any
import pandas as pd

from .sql_generator import generate_create_statements, save_sql

def compute_metrics(tables: Dict[str, pd.DataFrame]):
    metrics = {}
    for t, df in tables.items():
        metrics[t] = {}
        n = len(df)
        for col in df.columns:
            s = df[col]
            null_pct = float(s.isna().mean())
            unique_frac = float(s.dropna().nunique()) / max(1, len(s))
            dtype = str(s.dtype)
            metrics[t][col] = {
                "null_pct": null_pct,
                "unique_frac": unique_frac,
                "dtype": dtype,
                "sample": s.dropna().astype(str).head(5).tolist()
            }
    return metrics


def classify_columns(tables: Dict[str, pd.DataFrame], analysis: Dict, metrics=None):
    if metrics is None:
        metrics = compute_metrics(tables)
    pks = analysis.get("pks", {})
    fks = analysis.get("fks", {})
    types = analysis.get("types", {})
    result = {}
    for t, df in tables.items():
        result[t] = {}
        for col in df.columns:
            m = metrics[t][col]
            reason = []
            layer = "bronze"
            # long text or blob -> keep bronze
            if m["dtype"].startswith('object') and m["unique_frac"] < 0.02 and any(len(x) > 200 for x in m["sample"]):
                reason.append("long_text_blob -> keep bronze")
                layer = "bronze"
            elif pks.get(t) == col:
                reason.append("primary key -> silver (conformed)")
                layer = "silver"
            elif (t, col) in fks:
                reason.append("foreign key -> silver (joinable)")
                layer = "silver"
            elif m["null_pct"] < 0.5 and any(x in m["dtype"] for x in ['int','float','bool','datetime','object']):
                if any(x in m["dtype"] for x in ['int','float']) and m["unique_frac"] < 0.2:
                    reason.append("numeric measure -> gold")
                    layer = "gold"
                else:
                    reason.append("cleanable -> silver")
                    layer = "silver"
            else:
                reason.append("default: keep bronze")
                layer = "bronze"
            result[t][col] = {"layer": layer, "reason": "; ".join(reason)}
    return result


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def materialize_silver(
    tables: Dict[str, pd.DataFrame],
    analysis: Dict,
    out_dir: str = 'silver',
    layer_schema_map: Dict[str, str] | None = None,
) -> None:
    """Create basic cleaned/conformed CSVs under `out_dir`.

    - Coerces numeric types
    - Parses dates
    - Deduplicates on PK if available
    """
    # Now only generate SQL schema that references bronze. Use provided layer->schema mapping.
    os.makedirs(out_dir, exist_ok=True)
    classified = classify_columns(tables, analysis)
    # Build cleaned tables in-memory (select silver-appropriate columns)
    cleaned = {}
    for t, df in tables.items():
        cols = [c for c, meta in classified.get(t, {}).items() if meta['layer'] == 'silver']
        if not cols:
            continue
        cleaned[t] = df[cols].copy()
    # Default mapping if none provided
    if layer_schema_map is None:
        layer_schema_map = {'bronze': 'bronze', 'silver': 'silver', 'gold': 'gold'}
    schema_name = layer_schema_map.get('silver')
    _ensure_dir(out_dir)
    pks = analysis.get('pks', {})
    # Previously this function wrote cleaned CSVs to disk. Per request, we no longer
    # emit CSV files for the silver layer. Instead we perform light cleaning in-memory
    # (coercions, date parsing, trimming) so we can still compute schema information.
    cleaned_tables: Dict[str, pd.DataFrame] = {}
    for t, df in tables.items():
        df2 = df.copy()
        for col in df2.columns:
            # try numeric coercion
            if df2[col].dtype == object:
                try:
                    df2[col] = pd.to_numeric(df2[col], errors='ignore')
                except Exception:
                    pass
            # parse dates heuristically
            if 'date' in col.lower() or 'time' in col.lower():
                try:
                    df2[col] = pd.to_datetime(df2[col], errors='ignore')
                except Exception:
                    pass
            # trim strings
            if df2[col].dtype == object:
                df2[col] = df2[col].astype(str).str.strip()

        # dedupe on PK (in-memory only)
        pk = pks.get(t)
        if pk and pk in df2.columns:
            df2 = df2.drop_duplicates(subset=[pk])

        cleaned_tables[t] = df2

    # generate SQL schema for silver layer using the filtered cleaned frames
    try:
        if not cleaned:
            return
        sql_text = generate_create_statements(
            cleaned,
            analysis,
            table_names=list(cleaned.keys()),
            drop_if_exists=True,
            if_not_exists=True,
            schema_name=schema_name,
            ref_layer_name='bronze',
            layer_schema_map=layer_schema_map,
        )
        save_sql(sql_text, os.path.join(out_dir, 'schema.sql'))
    except Exception:
        pass


def materialize_bronze(
    tables: Dict[str, pd.DataFrame],
    analysis: Dict,
    out_dir: str = 'bronze',
    layer_schema_map: Dict[str, str] | None = None,
) -> None:
    """Materialize the bronze layer: raw tables, all columns, schema-qualified DDL only."""
    os.makedirs(out_dir, exist_ok=True)
    # All columns, raw tables
    bronze_tables = {t: df.copy() for t, df in tables.items()}
    # Default mapping if none provided
    if layer_schema_map is None:
        layer_schema_map = {'bronze': 'bronze', 'silver': 'silver', 'gold': 'gold'}
    schema_name = layer_schema_map.get('bronze', 'bronze')
    try:
        if not bronze_tables:
            return
        sql_text = generate_create_statements(
            bronze_tables,
            analysis,
            table_names=list(bronze_tables.keys()),
            drop_if_exists=True,
            if_not_exists=True,
            schema_name=schema_name,
            ref_layer_name=None,
            layer_schema_map=layer_schema_map,
        )
        save_sql(sql_text, os.path.join(out_dir, 'schema.sql'))
    except Exception:
        pass

def materialize_gold(
    tables: Dict[str, pd.DataFrame],
    analysis: Dict,
    out_dir: str = 'gold',
    layer_schema_map: Dict[str, str] | None = None,
) -> None:
    """Create simple gold artifacts (facts and dims) for the sample dataset.

    This implements a small example: builds `fact_sales` by joining `order_items` + `orders` + `products`.
    Also writes `dim_customer` and `dim_product` if present.
    """
    # Now compute artifacts and only generate SQL. Use provided layer->schema mapping.
    os.makedirs(out_dir, exist_ok=True)
    classified = classify_columns(tables, analysis)
    # Heuristic: largest table -> fact, others dims
    sizes = {t: len(df) for t, df in tables.items()}
    fact_table = max(sizes, key=lambda k: sizes[k])
    dim_tables = [t for t in tables.keys() if t != fact_table]
    # Build gold tables (dim_/fact_ prefixes) selecting gold-classified columns
    gold_tables = {}
    fact_cols = [c for c, meta in classified.get(fact_table, {}).items() if meta['layer'] == 'gold']
    if fact_cols:
        gold_tables[f'fact_{fact_table}'] = tables[fact_table][fact_cols].copy()
    for dt in dim_tables:
        cols = [c for c, meta in classified.get(dt, {}).items() if meta['layer'] == 'gold']
        if cols:
            gold_tables[f'dim_{dt}'] = tables[dt][cols].copy()
    _ensure_dir(out_dir)
    # Default mapping if none provided
    if layer_schema_map is None:
        layer_schema_map = {'bronze': 'bronze', 'silver': 'silver', 'gold': 'gold'}
    schema_name = layer_schema_map.get('gold')
    # write dims
    # Previously this function wrote dim/fact CSVs. Per request, we will compute
    # them in-memory but not persist CSV files. We still generate the gold schema
    # SQL based on classification below.
    gold_artifacts: Dict[str, pd.DataFrame] = {}
    if 'customers' in tables:
        cust = tables['customers'].copy()
        if 'customer_id' in cust.columns:
            cust = cust.drop_duplicates(subset=['customer_id'])
        gold_artifacts['dim_customer'] = cust

    if 'products' in tables:
        prod = tables['products'].copy()
        if 'product_id' in prod.columns:
            prod = prod.drop_duplicates(subset=['product_id'])
        gold_artifacts['dim_product'] = prod

    # build fact_sales in-memory if possible
    if 'order_items' in tables and 'orders' in tables:
        oi = tables['order_items'].copy()
        ords = tables['orders'].copy()
        merged = oi.merge(ords, on='order_id', how='left', suffixes=('_item', '_order'))
        if 'products' in tables and 'product_id' in oi.columns:
            merged = merged.merge(tables['products'], on='product_id', how='left')

        # compute line amount if possible
        if 'quantity' in merged.columns and 'unit_price' in merged.columns:
            try:
                merged['line_amount'] = merged['quantity'].astype(float) * merged['unit_price'].astype(float)
            except Exception:
                merged['line_amount'] = None

        # aggregate to fact level (order_id)
        if 'order_id' in merged.columns:
            fact = merged.groupby(['order_id']).agg({'line_amount': 'sum'}).reset_index()
            gold_artifacts['fact_sales'] = fact
        else:
            gold_artifacts['fact_sales_raw'] = merged

    # generate SQL schema for gold layer (combine computed gold frames and artifacts)
    try:
        final_gold_frames: Dict[str, pd.DataFrame] = {}
        final_gold_frames.update(gold_tables or {})
        final_gold_frames.update(gold_artifacts or {})
        if not final_gold_frames:
            return
        sql_text = generate_create_statements(
            final_gold_frames,
            analysis,
            table_names=list(final_gold_frames.keys()),
            drop_if_exists=True,
            if_not_exists=True,
            schema_name=schema_name,
            ref_layer_name='silver',
            layer_schema_map=layer_schema_map,
        )
        save_sql(sql_text, os.path.join(out_dir, 'schema.sql'))
    except Exception:
        pass

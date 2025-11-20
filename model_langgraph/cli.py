import argparse
import os
from .loader import load_input
from .analyzer import analyze
from .sql_generator import generate_create_statements, save_sql
from .erd import render_erd
from .catalog import build_catalog, save_catalog, convert_to_star_schema

def main():
    parser = argparse.ArgumentParser(description='Generate dimensional model from CSV/JSON using LangGraph')
    parser.add_argument('input', help='Input file or directory (CSV/JSON)')
    parser.add_argument('--out', help='Output .sql file path', default='model.sql')
    parser.add_argument('--erd', help='Output ERD path (without extension)', default='erd')
    parser.add_argument('--catalog', help='Output catalog JSON path', default='catalog.json')
    parser.add_argument('--star', help='Output star schema JSON path', default=None)
    parser.add_argument('--langgraph', help='Output LangGraph JSON path', default=None)
    parser.add_argument('--materialize-bronze', help='Materialize bronze layer SQL', action='store_true')
    parser.add_argument('--materialize-silver', help='Materialize silver layer SQL', action='store_true')
    parser.add_argument('--materialize-gold', help='Materialize gold layer SQL', action='store_true')
    parser.add_argument('--layer-schema-map', help='Layer to schema mapping as comma-separated pairs, e.g. bronze=bronze,silver=silver,gold=gold', default=None)
    args = parser.parse_args()

    tables = load_input(args.input)
    analysis = analyze(tables)

    # Build per-table schema mapping using classification and optional layer->schema map
    from .layers import classify_columns
    layer_schema_map = None
    if args.layer_schema_map:
        layer_schema_map = dict(pair.split('=') for pair in args.layer_schema_map.split(','))
    else:
        layer_schema_map = {'bronze': 'bronze', 'silver': 'silver', 'gold': 'gold'}

    col_class = classify_columns(tables, analysis)
    # simple table-level layer: prefer gold if any gold cols, else silver if any silver cols, else bronze
    table_layer: Dict[str, str] = {}
    for t, cols in col_class.items():
        layers = {m['layer'] for m in cols.values()}
        if 'gold' in layers:
            table_layer[t] = 'gold'
        elif 'silver' in layers:
            table_layer[t] = 'silver'
        else:
            table_layer[t] = 'bronze'

    table_schema_map = {t: layer_schema_map.get(table_layer.get(t, 'bronze')) for t in tables.keys()}

    # Generate DDL grouped by schema to emit CREATE SCHEMA headers per schema
    parts = []
    # group tables by schema name
    grouped: Dict[str, list] = {}
    for t, sch in table_schema_map.items():
        grouped.setdefault(sch, []).append(t)

    for sch, tlist in grouped.items():
        part = generate_create_statements(
            tables,
            analysis,
            table_names=tlist,
            drop_if_exists=False,
            if_not_exists=True,
            schema_name=sch,
            table_schema_map=table_schema_map,
            layer_schema_map=layer_schema_map,
        )
        parts.append(part)

    sql_text = '\n\n'.join(parts)
    os.makedirs(os.path.dirname(args.out) or '.', exist_ok=True)
    save_sql(sql_text, args.out)

    # ERD render
    os.makedirs(os.path.dirname(args.erd) or '.', exist_ok=True)
    try:
        render_erd(tables, analysis, args.erd)
    except Exception as e:
        print('ERD generation failed:', e)

    # catalog
    catalog = build_catalog(tables)
    save_catalog(catalog, args.catalog)

    if args.star:
        star = convert_to_star_schema(tables, analysis)
        import json
        with open(args.star, 'w', encoding='utf8') as fh:
            json.dump(star, fh, indent=2)

    if args.langgraph:
        from .analyzer import build_langgraph_model, save_langgraph_model_json
        lg_model = build_langgraph_model(tables, analysis)
        save_langgraph_model_json(lg_model, args.langgraph)
        print('LangGraph JSON saved to', args.langgraph)


    # materialize bronze/silver/gold
    if args.materialize_bronze:
        from .layers import materialize_bronze
        layer_schema_map = None
        if args.layer_schema_map:
            layer_schema_map = dict(pair.split('=') for pair in args.layer_schema_map.split(','))
        materialize_bronze(tables, analysis, out_dir='bronze', layer_schema_map=layer_schema_map)
        print('Bronze layer materialized in ./bronze')

    if args.materialize_silver:
        from .layers import materialize_silver
        layer_schema_map = None
        if args.layer_schema_map:
            layer_schema_map = dict(pair.split('=') for pair in args.layer_schema_map.split(','))
        materialize_silver(tables, analysis, out_dir='silver', layer_schema_map=layer_schema_map)
        print('Silver layer materialized in ./silver')

    if args.materialize_gold:
        from .layers import materialize_gold
        layer_schema_map = None
        if args.layer_schema_map:
            layer_schema_map = dict(pair.split('=') for pair in args.layer_schema_map.split(','))
        materialize_gold(tables, analysis, out_dir='gold', layer_schema_map=layer_schema_map)
        print('Gold layer materialized in ./gold')

    print('Done. SQL saved to', args.out)
    print('Catalog saved to', args.catalog)
    if args.star:
        print('Star schema saved to', args.star)

if __name__ == '__main__':
    main()

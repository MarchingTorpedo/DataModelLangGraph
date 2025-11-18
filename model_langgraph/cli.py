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
    parser.add_argument('--materialize-silver', help='Materialize silver layer CSVs', action='store_true')
    parser.add_argument('--materialize-gold', help='Materialize gold layer CSVs', action='store_true')
    args = parser.parse_args()

    tables = load_input(args.input)
    analysis = analyze(tables)

    sql_text = generate_create_statements(tables, analysis)
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

    # materialize silver/gold
    if args.materialize_silver:
        from .layers import materialize_silver
        materialize_silver(tables, analysis, out_dir='silver')
        print('Silver layer materialized in ./silver')

    if args.materialize_gold:
        from .layers import materialize_gold
        materialize_gold(tables, analysis, out_dir='gold')
        print('Gold layer materialized in ./gold')

    print('Done. SQL saved to', args.out)
    print('Catalog saved to', args.catalog)
    if args.star:
        print('Star schema saved to', args.star)

if __name__ == '__main__':
    main()

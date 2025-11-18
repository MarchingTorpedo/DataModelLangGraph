from graphviz import Digraph
from typing import Dict, Tuple

def render_erd(tables: Dict[str, object], analysis: Dict, out_path: str):
    dot = Digraph(comment='ERD', format='png')
    # add nodes with simple table layouts
    for t, df in tables.items():
        label = f"{t}|"
        label += '\\l'.join([str(c) for c in df.columns]) + '\\l'
        dot.node(t, label='{' + label + '}', shape='record')

    for (tbl, col), (ref_tbl, ref_col) in analysis.get('fks', {}).items():
        dot.edge(ref_tbl, tbl, label=f'{ref_col} -> {col}')

    dot.render(out_path, cleanup=True)

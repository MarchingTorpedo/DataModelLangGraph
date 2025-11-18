import os
import json
import pandas as pd
from typing import Dict

def load_input(path: str) -> Dict[str, pd.DataFrame]:
    """Load CSV or JSON input. If path is a directory, load all CSV/JSON files.

    Returns a dict of table_name -> DataFrame
    """
    if os.path.isdir(path):
        files = [os.path.join(path, f) for f in os.listdir(path) if f.lower().endswith(('.csv', '.json'))]
    else:
        files = [path]

    tables = {}
    for f in files:
        name = os.path.splitext(os.path.basename(f))[0]
        if f.lower().endswith('.csv'):
            df = pd.read_csv(f)
            tables[name] = df
        elif f.lower().endswith('.json'):
            with open(f, 'r', encoding='utf8') as fh:
                j = json.load(fh)
            # support either list (table) or dict of tables
            if isinstance(j, list):
                tables[name] = pd.DataFrame(j)
            elif isinstance(j, dict):
                # assume dict of named tables
                for k, v in j.items():
                    tables[k] = pd.DataFrame(v)
            else:
                # fallback: wrap
                tables[name] = pd.DataFrame([j])
    return tables

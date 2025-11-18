ModelLangGraph — Dimensional Data Model Generator using LangGraph

This project converts input CSV/JSON files into a dimensional data model and outputs SQL, ERD diagrams, and metadata catalogs.

Features
- Detects CSV or JSON input automatically
- Automatic foreign key detection (heuristics)
- Column description generation (local AI model if available, otherwise heuristics)
- ERD generation using Graphviz
- JSON star schema conversion
- CSV data catalog metadata builder
- Save model as `.sql`
- Explicitly uses `LangGraph` for internal modeling

Requirements
- Python 3.9+
- Install core deps:

```pwsh
pip install -r requirements.txt
```

Optional (local AI for descriptions):
- `transformers` + a local text-generation model or set `LOCAL_LLM_MODEL` to a local path.

Quick run

```pwsh
# Example using a sample folder
python -m model_langgraph.cli samples/customers.csv --out out/model.sql --erd out/erd.png --catalog out/catalog.json
```

Project layout
- `model_langgraph/` — main package
- `samples/` — example CSV/JSON files demonstrating relationships
- `run_examples.ps1` — convenience script for Windows PowerShell

Notes
- This is designed for local-first usage — no API keys required. If you have a local LLM you can set `LOCAL_LLM_MODEL` env var to point to it.
- A small `mcp_stub.py` file is included to ease later MCP server integration.

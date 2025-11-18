# Run the sample pipeline on the provided samples folder
# This runs the full pipeline and materializes Silver and Gold layers
python -m model_langgraph.cli samples --materialize-silver --materialize-gold --out out/model.sql --erd out/erd --catalog out/catalog.json --star out/star.json --langgraph out/langgraph.json

Write-Host "Finished. Outputs are in ./out (and ./silver, ./gold)"

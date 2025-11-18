# Run the sample pipeline on the provided samples folder
python -m model_langgraph.cli samples --out out/model.sql --erd out/erd --catalog out/catalog.json --star out/star.json

Write-Host "Finished. Outputs are in ./out"

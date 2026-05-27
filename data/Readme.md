# Data Sources

The Steam dataset used in this project is publicly available from the following S3 bucket:

s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json

The raw dataset is intentionally not versioned inside this repository due to size considerations.

All Bronze, Silver, and Gold Delta tables are generated directly inside Databricks during pipeline execution.
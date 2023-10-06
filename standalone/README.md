# Tangara Standalone Pipeline
The Tangara Pipeline standalone version is an easy way to build an ETL data pipeline from scratch. This version is a brief solution using Jupyter Notebooks, Python, and Bash scripts to generate output data from data sources provided by Air Quality sensors in Cali, Colombia. https://tangara.chis.pa/

## Data
Read more [README.md](data/README.md)

## Notebooks
Read more [README.md](notebooks/README.md)

## Scripts
Read more [README.md](scripts/README.md)

## Documentation
Read more [README.md](docs/README.md)

# Pipelines

Execute each pipeline from the tangara-pipeline directory root.

## Last 5 Minutes

Process data from the last 5 minutes, execute this script every 5 minutes to report data from the last 5 minutes since the current datetime:

```bash
# from tangara-pipeline root directory
$promt> ./standalone/scripts/last_5min.sh
```

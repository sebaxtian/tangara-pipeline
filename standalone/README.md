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

## Environment Variables

Please setup each environment variable value before execute any script or notebook:

Create a new **.env** file inside the **standalone** directory and use the environment variables as you can see in the **example.env** file just change the values. This file will be ignored and never will be committed to the repository.

The environement variables below are required:

* URL_INFLUXDB_QUERY_ENDPOINT=http://influxdb.example:8080/query
* DB_NAME_INFLUXDB=my_database
* PLOT_CHARTS=
* GROUP_BY_TIME=30s
* START_ISO8601_DATETIME=2023-03-17T00:00:00-05:00
* END_ISO8601_DATETIME=2023-03-17T00:00:00-05:00

**GROUP_BY_TIME** allowed values: **30s**, **1m**, **1h**

When are you using 30s the raw data collected from InfluxDB will be the last value every 30 seconds, if you use another value 1m or 1h the raw data collected from InfluxDB will be the mean value every 1 minute or every 1 hour.

# Pipelines

Execute each pipeline from the tangara-pipeline directory root.

## Last 5 Minutes

Process data from the last 5 minutes, execute this script every 5 minutes to report data from the last 5 minutes since the current datetime:

```bash
# from tangara-pipeline root directory
$promt> ./standalone/scripts/last_5min.sh
```

## Last Hour

Process data from the last hour, and execute this script every hour to report data from the last hour since the current datetime:

```bash
# from tangara-pipeline root directory
$promt> ./standalone/scripts/last_hour.sh
```

## Last 4 Hours

Process data from the last 4 hours, and execute this script every 4 hours to report data from the last 4 hours since the current datetime:

```bash
# from tangara-pipeline root directory
$promt> ./standalone/scripts/last_4h.sh
```

## Last 8 Hours

Process data from the last 8 hours, and execute this script every 8 hours to report data from the last 8 hours since the current datetime:

```bash
# from tangara-pipeline root directory
$promt> ./standalone/scripts/last_8h.sh
```

## Last 12 Hours

Process data from the last 12 hours, and execute this script every 12 hours to report data from the last 12 hours since the current datetime:

```bash
# from tangara-pipeline root directory
$promt> ./standalone/scripts/last_12h.sh
```

## X Date Time

Process data from any date time interval:

```bash
# from tangara-pipeline root directory
$promt> ./standalone/scripts/x_time.sh
```

## Dia sin Carro y sin Moto

Please, before run the dscysm.sh setup the .env file, to process data it needs 48 hours of data:

```bash
# .env file variables
# Example:
PLOT_CHARTS=
GROUP_BY_TIME=30s
START_ISO8601_DATETIME=2023-11-07T00:00:00-05:00
END_ISO8601_DATETIME=2023-11-08T23:59:59-05:00
```

Finally, process the data:

```bash
# from tangara-pipeline root directory
$promt> ./standalone/scripts/dscysm.sh
```

# Tangara Scripts

Execute each pipeline script from the tangara-pipeline directory root.

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

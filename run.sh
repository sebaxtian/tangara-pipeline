#!/bin/bash

#
# Run tangara-pipeline
#

echo "Running Tangara Pipeline ..."

source .venv/bin/activate

# Run PM25 Pipeline
DATETIME=$(date '+%Y-%m-%dT%H:%M:%S')
echo 'DATETIME: '$DATETIME
kedro run --params nowcast_datetime:$DATETIME
# '2022-09-06T13:35:00'
# kedro run --params nowcast_datetime:'2022-09-14T09:13:25'

# Run PM25 NowCast AQI
kedro run --pipeline pm25_nowcast_aqi

echo "Finished !!"

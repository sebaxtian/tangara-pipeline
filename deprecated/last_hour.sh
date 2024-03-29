#!/bin/bash

#
# Run tangara-pipeline
#
# Process data from last hour, execute this script every 5 minutes to report data from
# the last hour since current datetime.
#
# Please, before run, setup the credentials.yml file into directory conf/local/
# Credentials to InfluxDB
# influxdb:
#    url: VALUE
#    token: VALUE
#    org: VALUE
#    bucket: VALUE
#    username: VALUE
#    password: VALUE
#    database: VALUE
#

# Change to tangara-pipeline Directory
if [[ -z $1 ]];
then 
    echo "tangara-pipeline Directory: $PWD"
    cd $PWD
else
    echo "tangara-pipeline Directory: $1"
    cd $1
fi

# Setup Virtual Environment
#python -m venv .venv

# Activate Virtual Enviromment
source .venv/bin/activate

# Install dependencies
#python -m pip install --upgrade pip
#if [ -f src/requirements.txt ]; then pip install -r src/requirements.txt; fi

echo "Running Tangara Pipeline ..."

# Run Tangara Stations Pipeline
NOWCAST_DATETIME=$(TZ='America/Bogota' date --iso-8601=seconds)
START_DATETIME=$(TZ='America/Bogota' date --date='1 hour ago' --iso-8601=seconds)
echo 'NOWCAST_DATETIME: '$NOWCAST_DATETIME
echo 'START_DATETIME: '$START_DATETIME

# Run Tangara Stations
kedro run --params "nowcast_datetime:$NOWCAST_DATETIME, start_datetime:$START_DATETIME"

# Run PM25
kedro run --pipeline pm25

# Run PM25 NowCast AQI
kedro run --pipeline pm25_nowcast_aqi

# Run InfluxDB Tangara Stations
kedro run --pipeline influxdb_tangara_stations

echo "Finished !!"

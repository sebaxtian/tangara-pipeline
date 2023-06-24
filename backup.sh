#!/bin/bash

#
# Run tangara-pipeline
#
# Process data from dates to get a backup
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
python -m venv .venv

# Activate Virtual Enviromment
source .venv/bin/activate

# Install dependencies
python -m pip install --upgrade pip
if [ -f src/requirements.txt ]; then pip install -r src/requirements.txt; fi

echo "Running Tangara Pipeline ..."

# Clean Backup Data
rm -rfv data/09_backup/*

NOWCAST_DATETIME='2023-05-31'
START_DATETIME='2023-05-01'
echo 'NOWCAST_DATETIME: '$NOWCAST_DATETIME
echo 'START_DATETIME: '$START_DATETIME

# After this, START_DATETIME and NOWCAST_DATETIME will be valid ISO 8601 dates,
# or the script will have aborted when it encountered unparseable data
# such as input_end=abcd
START_DATETIME=$(date -I -d "$START_DATETIME") || exit -1
NOWCAST_DATETIME=$(date -I -d "$NOWCAST_DATETIME") || exit -1

END=$(date -I -d "$NOWCAST_DATETIME + 1 day")
START="$START_DATETIME"
while [ "$START" != "$END" ]; do
    # Date Time Period
    START_DATETIME=$START"T00:00:00-05:00"
    END_DATETIME=$START"T23:59:59-05:00"
    echo "START_DATETIME: $START_DATETIME"
    echo "END_DATETIME: $END_DATETIME"

    # Clean Data
    rm -rfv data/01_raw/*
    rm -rfv data/02_intermediate/*
    rm -rfv data/03_primary/*
    rm -rfv data/04_feature/*

    # Run Tangara Stations Pipeline
    kedro run --params "nowcast_datetime:$END_DATETIME, start_datetime:$START_DATETIME"

    # Run PM25
    kedro run --pipeline pm25

    # Run PM25 NowCast AQI
    kedro run --pipeline pm25_nowcast_aqi

    # Run InfluxDB Tangara Stations
    kedro run --pipeline influxdb_tangara_stations

    # Make Backup
    mkdir data/09_backup/$START
    cp -rfv data/01_raw data/09_backup/$START && rm -rfv data/09_backup/$START/01_raw/.gitkeep
    cp -rfv data/02_intermediate data/09_backup/$START && rm -rfv data/09_backup/$START/02_intermediate/.gitkeep
    cp -rfv data/03_primary data/09_backup/$START && rm -rfv data/09_backup/$START/03_primary/.gitkeep
    cp -rfv data/04_feature data/09_backup/$START && rm -rfv data/09_backup/$START/04_feature/.gitkeep
    
    # Next Day
    START=$(date -I -d "$START + 1 day")
done

echo "Finished !!"

#!/bin/bash

#
# Run tangara-pipeline
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

echo "Running Tangara Pipeline ..."

# Run PM25 Pipeline
NOWCAST_DATETIME=$(TZ='America/Bogota' date '+%Y-%m-%dT%H:%M:%S')
echo 'NOWCAST_DATETIME: '$NOWCAST_DATETIME
kedro run --params nowcast_datetime:$NOWCAST_DATETIME
# '2022-09-06T13:35:00'
# kedro run --params nowcast_datetime:'2022-09-14T09:13:25'

# Run PM25
kedro run --pipeline pm25

# Run PM25 NowCast AQI
kedro run --pipeline pm25_nowcast_aqi

# Run PM25 InfluxDB AQI
#kedro run --pipeline pm25_influxdb_aqi

echo "Finished !!"

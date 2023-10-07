#!/bin/bash

#
# Run tangara-pipeline
#
# Process data from the last hour, and execute this script every hour
# to report data from the last hour since the current datetime.
#

echo "Running Tangara Last Hour Pipeline ..."

# Define datetime interval to last 5min from now
START_ISO8601_DATETIME=$(TZ='America/Bogota' date --date='1 hour ago' --iso-8601=seconds)
END_ISO8601_DATETIME=$(TZ='America/Bogota' date --iso-8601=seconds)
GROUP_BY_TIME='1m'
echo 'START_ISO8601_DATETIME: '$START_ISO8601_DATETIME
echo 'END_ISO8601_DATETIME: '$END_ISO8601_DATETIME
echo 'GROUP_BY_TIME: '$GROUP_BY_TIME

# Update .env file with new datetime interval
sed -i~ "/^START_ISO8601_DATETIME=/s/=.*/="$START_ISO8601_DATETIME"/" standalone/.env
sed -i~ "/^END_ISO8601_DATETIME=/s/=.*/="$END_ISO8601_DATETIME"/" standalone/.env
sed -i~ "/^GROUP_BY_TIME=/s/=.*/="$GROUP_BY_TIME"/" standalone/.env

# Run Tangara Stations
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/tangaras.ipynb

# Run Temperature Raw Data
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/temp_raw.ipynb

# Run Humidity Raw Data
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/hum_raw.ipynb

# Run PM2.5 Raw Data
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/pm25_raw.ipynb

# Run PM2.5 Clean Data
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/pm25_clean.ipynb

# Run PM2.5 to AQI
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/pm25_to_aqi.ipynb

echo "Finished !!"

#!/bin/bash

#
# Run tangara-pipeline
#
# Process data since the START_ISO8601_DATETIME environment variable value,
# to END_ISO8601_DATETIME environment variable value
#
# Please, read the standalone/README.md file for details.
#

echo "Running Tangara Pipeline ..."

# Define datetime interval
START_ISO8601_DATETIME=$(awk -F= -v key="START_ISO8601_DATETIME" '$1==key {print $2}' standalone/.env)
END_ISO8601_DATETIME=$(awk -F= -v key="END_ISO8601_DATETIME" '$1==key {print $2}' standalone/.env)
echo 'START_ISO8601_DATETIME: '$START_ISO8601_DATETIME
echo 'END_ISO8601_DATETIME: '$END_ISO8601_DATETIME

# Run Tangara Stations
echo 'Running Tangara Stations ...'
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/tangaras.ipynb &>/dev/null
echo '... OK'

# Run Temperature Raw Data
echo 'Running Temperature Raw Data ...'
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/temp_raw.ipynb &>/dev/null
echo '... OK'

# Run Humidity Raw Data
echo 'Running Humidity Raw Data ...'
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/hum_raw.ipynb &>/dev/null
echo '... OK'

# Run PM2.5 Raw Data
echo 'Running PM2.5 Raw Data ...'
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/pm25_raw.ipynb &>/dev/null
echo '... OK'

# Run PM2.5 Clean Data
echo 'Running PM2.5 Clean Data ...'
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/pm25_clean.ipynb &>/dev/null
echo '... OK'

# Run PM2.5 to AQI
echo 'Running PM2.5 to AQI ...'
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/pm25_to_aqi.ipynb &>/dev/null
echo '... OK'

echo "Finished !!"

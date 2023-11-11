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
echo ''

# Clean up
directory=$(echo $END_ISO8601_DATETIME | cut -d 'T' -f 1)
rm -rf standalone/data/dia_sin_carro_y_sin_moto/$directory
mkdir standalone/data/dia_sin_carro_y_sin_moto/$directory

# Run Tangara Stations
echo 'Running Tangara Stations ...'
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/tangaras.ipynb &>/dev/null
cp -fv standalone/data/0_raw/tangaras.csv standalone/data/dia_sin_carro_y_sin_moto/$directory/tangaras.csv
echo '... OK'
echo ''

# Run Temperature Raw Data
echo 'Running Temperature Raw Data ...'
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/temp_raw.ipynb &>/dev/null
cp -fv standalone/data/0_raw/temp_raw.csv standalone/data/dia_sin_carro_y_sin_moto/$directory/temp_raw.csv
echo '... OK'
echo ''

# Run Humidity Raw Data
echo 'Running Humidity Raw Data ...'
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/hum_raw.ipynb &>/dev/null
cp -fv standalone/data/0_raw/hum_raw.csv standalone/data/dia_sin_carro_y_sin_moto/$directory/hum_raw.csv
echo '... OK'
echo ''

# Run PM2.5 Raw Data
echo 'Running PM2.5 Raw Data ...'
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/pm25_raw.ipynb &>/dev/null
cp -fv standalone/data/0_raw/pm25_raw.csv standalone/data/dia_sin_carro_y_sin_moto/$directory/pm25_raw.csv
echo '... OK'
echo ''

# Run PM2.5 Clean Data
echo 'Running PM2.5 Clean Data ...'
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/pm25_clean.ipynb &>/dev/null
cp -fv standalone/data/1_clean/pm25_clean.csv standalone/data/dia_sin_carro_y_sin_moto/$directory/pm25_clean.csv
echo '... OK'
echo ''

# Run PM2.5 to AQI
echo 'Running PM2.5 to AQI ...'
jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/pm25_to_aqi.ipynb &>/dev/null
cp -fv standalone/data/2_features/aqi.csv standalone/data/dia_sin_carro_y_sin_moto/$directory/aqi.csv
echo '... OK'
echo ''

echo "Finished !!"

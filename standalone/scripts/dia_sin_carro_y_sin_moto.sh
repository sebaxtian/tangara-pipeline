#!/bin/bash

#
# Run tangara-pipeline
#
# Getting data to analyze Dia Sin Carro y Sin Moto.
#
# Process data since the START_ISO8601_DATETIME environment variable value,
# to END_ISO8601_DATETIME environment variable value.
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
directory=$(echo $START_ISO8601_DATETIME | cut -d 'T' -f 1)
rm -rf standalone/data/dia_sin_carro_y_sin_moto/$directory
mkdir standalone/data/dia_sin_carro_y_sin_moto/$directory

# Loop in datetime interval, every hour
next_datetime=$START_ISO8601_DATETIME
end_datetime=$END_ISO8601_DATETIME

while [[ $next_datetime < $end_datetime ]]; do

    # suffix file name
    suffix_filename=$(echo $next_datetime | cut -d 'T' -f 2 | cut -d '-' -f 1)

    # Update .env file with new datetime interval
    sed -i~ "/^START_ISO8601_DATETIME=/s/=.*/="$next_datetime"/" standalone/.env
    echo 'START_ISO8601_DATETIME: '$next_datetime

    # Update next datetime +1 hour
    next_datetime=$(TZ='America/Bogota' date --date=$next_datetime' +1 hour' --iso-8601=seconds)

    sed -i~ "/^END_ISO8601_DATETIME=/s/=.*/="$next_datetime"/" standalone/.env
    echo 'END_ISO8601_DATETIME: '$next_datetime

    echo ''


    # Run Tangara Stations
    echo 'Running Tangara Stations ...'
    jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/tangaras.ipynb &>/dev/null
    # Copy file
    cp -fv standalone/data/0_raw/tangaras.csv standalone/data/dia_sin_carro_y_sin_moto/$directory/tangaras_$suffix_filename.csv
    echo '... OK'
    echo ''

    # Run Temperature Raw Data
    echo 'Running Temperature Raw Data ...'
    jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/temp_raw.ipynb &>/dev/null
    # Copy file
    cp -fv standalone/data/0_raw/temp_raw.csv standalone/data/dia_sin_carro_y_sin_moto/$directory/temp_raw_$suffix_filename.csv
    echo '... OK'
    echo ''

    # Run Humidity Raw Data
    echo 'Running Humidity Raw Data ...'
    jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/hum_raw.ipynb &>/dev/null
    # Copy file
    cp -fv standalone/data/0_raw/hum_raw.csv standalone/data/dia_sin_carro_y_sin_moto/$directory/hum_raw_$suffix_filename.csv
    echo '... OK'
    echo ''

    # Run PM2.5 Raw Data
    echo 'Running PM2.5 Raw Data ...'
    jupyter nbconvert --execute --to notebook --inplace standalone/notebooks/pm25_raw.ipynb &>/dev/null
    # Copy file
    cp -fv standalone/data/0_raw/pm25_raw.csv standalone/data/dia_sin_carro_y_sin_moto/$directory/pm25_raw_$suffix_filename.csv
    echo '... OK'
    echo ''

done


# Update .env file with original datetime interval
sed -i~ "/^START_ISO8601_DATETIME=/s/=.*/="$START_ISO8601_DATETIME"/" standalone/.env
sed -i~ "/^END_ISO8601_DATETIME=/s/=.*/="$END_ISO8601_DATETIME"/" standalone/.env


# Merge, Remove and Copy Tangaras
csvstack standalone/data/dia_sin_carro_y_sin_moto/$directory/tangaras_*.csv > standalone/data/dia_sin_carro_y_sin_moto/$directory/tangaras.csv
rm -fv standalone/data/dia_sin_carro_y_sin_moto/$directory/tangaras_*.csv
cp -fv standalone/data/dia_sin_carro_y_sin_moto/$directory/tangaras.csv standalone/data/0_raw/tangaras.csv
sort standalone/data/dia_sin_carro_y_sin_moto/$directory/tangaras.csv | uniq > /tmp/salida.csv && mv -fv /tmp/salida.csv standalone/data/dia_sin_carro_y_sin_moto/$directory/tangaras.csv
echo ''

# Merge, Remove and Copy Temperature Raw Data
csvstack standalone/data/dia_sin_carro_y_sin_moto/$directory/temp_raw_*.csv > standalone/data/dia_sin_carro_y_sin_moto/$directory/temp_raw.csv
rm -fv standalone/data/dia_sin_carro_y_sin_moto/$directory/temp_raw_*.csv
cp -fv standalone/data/dia_sin_carro_y_sin_moto/$directory/temp_raw.csv standalone/data/0_raw/temp_raw.csv
echo ''

# Merge, Remove and Copy Humidity Raw Data
csvstack standalone/data/dia_sin_carro_y_sin_moto/$directory/hum_raw_*.csv > standalone/data/dia_sin_carro_y_sin_moto/$directory/hum_raw.csv
rm -fv standalone/data/dia_sin_carro_y_sin_moto/$directory/hum_raw_*.csv
cp -fv standalone/data/dia_sin_carro_y_sin_moto/$directory/hum_raw.csv standalone/data/0_raw/hum_raw.csv
echo ''

# Merge, Remove and Copy PM2.5 Raw Data
csvstack standalone/data/dia_sin_carro_y_sin_moto/$directory/pm25_raw_*.csv > standalone/data/dia_sin_carro_y_sin_moto/$directory/pm25_raw.csv
rm -fv standalone/data/dia_sin_carro_y_sin_moto/$directory/pm25_raw_*.csv
cp -fv standalone/data/dia_sin_carro_y_sin_moto/$directory/pm25_raw.csv standalone/data/0_raw/pm25_raw.csv
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
cp -fv standalone/data/1_clean/aqi.csv standalone/data/dia_sin_carro_y_sin_moto/$directory/aqi.csv
echo '... OK'
echo ''


echo "Finished !!"

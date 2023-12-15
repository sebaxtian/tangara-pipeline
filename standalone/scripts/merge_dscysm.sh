#!/bin/bash

#
# Run merge-dscysm
#
# Merge all data from dia_sin_carro_y_sin_moto into single files by metric and category,
#
# Please, read the standalone/README.md file for details.
#

echo "Merging data from dia_sin_carro_y_sin_moto ..."
echo ''

echo "Merging Tangaras ..."
mlr --csv unsparsify $(ls standalone/data/dia_sin_carro_y_sin_moto/2023-*/tangaras.csv) > standalone/data/dscysm/2023/tangaras.csv
sort standalone/data/dscysm/2023/tangaras.csv | uniq > /tmp/salida.csv && mv -fv /tmp/salida.csv standalone/data/dscysm/2023/tangaras.csv
echo "... OK"
echo ''

echo "Merging Temperature Raw Data ..."
mlr --csv unsparsify $(ls standalone/data/dia_sin_carro_y_sin_moto/2023-*/temp_raw.csv) > standalone/data/dscysm/2023/temp_raw.csv
echo "... OK"
echo ''

echo "Merging Humidity Raw Data ..."
mlr --csv unsparsify $(ls standalone/data/dia_sin_carro_y_sin_moto/2023-*/hum_raw.csv) > standalone/data/dscysm/2023/hum_raw.csv
echo "... OK"
echo ''

echo "Merging PM2.5 Raw Data ..."
mlr --csv unsparsify $(ls standalone/data/dia_sin_carro_y_sin_moto/2023-*/pm25_raw.csv) > standalone/data/dscysm/2023/pm25_raw.csv
echo "... OK"
echo ''

echo "Merging PM2.5 Clean Data ..."
mlr --csv unsparsify $(ls standalone/data/dia_sin_carro_y_sin_moto/2023-*/pm25_clean.csv) > standalone/data/dscysm/2023/pm25_clean.csv
echo "... OK"
echo ''

echo "Merging AQI Data ..."
mlr --csv unsparsify $(ls standalone/data/dia_sin_carro_y_sin_moto/2023-*/aqi.csv) > standalone/data/dscysm/2023/aqi.csv
echo "... OK"
echo ''

echo "Finished !!"

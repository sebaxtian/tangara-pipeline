"""
This is a boilerplate pipeline 'influxdb_tangara_stations'
generated using Kedro 0.18.1
"""

import pandas as pd
from pathlib import Path
from influxdb_client import InfluxDBClient, Point
from kedro.config import ConfigLoader
from kedro.framework.project import settings

import reactivex as rx
from reactivex import operators as ops
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write.retry import WritesRetry
from influxdb_client.client.write_api import SYNCHRONOUS


# Get Tangara Stations Measurements Dictionary
def get_stations_measurements(
    stations: pd.DataFrame,
    pm25: pd.DataFrame,
    aqi: pd.DataFrame,
    temp: pd.DataFrame,
    hum: pd.DataFrame,
    co2: pd.DataFrame,
) -> pd.DataFrame: # pragma: no cover
    # Tangara Stations Measurements Dictionary
    stations_measurements = {}
    # For each Tangara Station
    for station in stations.itertuples():
        # print(station._fields)
        # print(getattr(station, 'ID'))
        station_id = getattr(station, "ID")
        # print('station_id:', station_id)

        # Set Index
        station_pm25 = pm25[["DATETIME", station_id]].set_index("DATETIME")
        station_aqi = aqi[["DATETIME", station_id]].set_index("DATETIME")
        station_temp = temp[["DATETIME", station_id]].set_index("DATETIME")
        station_hum = hum[["DATETIME", station_id]].set_index("DATETIME")
        station_co2 = co2[["DATETIME", station_id]].set_index("DATETIME")

        # Join Measurements
        pm25_aqi = station_pm25.join(station_aqi, lsuffix="_PM25", rsuffix="_AQI")
        temp_hum = station_temp.join(station_hum, lsuffix="_TEMP", rsuffix="_HUM")
        station_measurements = pm25_aqi.join([temp_hum, station_co2])

        # Rename Columns
        station_measurements.rename(
            columns={
                f"{station_id}_PM25": "PM25",
                f"{station_id}_AQI": "AQI",
                f"{station_id}_TEMP": "TEMP",
                f"{station_id}_HUM": "HUM",
                f"{station_id}": "CO2",
            },
            inplace=True,
        )
        # Reset Index
        station_measurements = station_measurements.reset_index()

        # print('station_measurements:', station_measurements.columns.to_list())

        # Add ID, MAC and GEOHASH columns
        station_measurements["STATION_ID"] = station_id
        station_measurements["MAC"] = getattr(station, "MAC")
        station_measurements["GEOHASH"] = getattr(station, "GEOHASH")

        # Set Tangara Station Measurements
        stations_measurements[station_id] = station_measurements

    return stations_measurements


def station_measurements_to_generator(station_measurements: pd.DataFrame) -> Point: # pragma: no cover
    """
    Parse your stations_measurements Data Frame into generator
    """
    # For each station_measurements tuple
    for row in station_measurements.itertuples():
        # print(row._fields)
        measurements = {
            "MEASUREMENT_NAME": "TANGARA_STATIONS",
            "STATION_ID": getattr(row, "STATION_ID"),
            "MAC": getattr(row, "MAC"),
            "GEOHASH": getattr(row, "GEOHASH"),
            "PM25": getattr(row, "PM25"),
            "AQI": getattr(row, "AQI"),
            "TEMP": getattr(row, "TEMP"),
            "HUM": getattr(row, "HUM"),
            "CO2": getattr(row, "CO2"),
            "DATETIME": getattr(row, "DATETIME"),
        }
        point = Point.from_dict(
            measurements,
            record_measurement_key="MEASUREMENT_NAME",
            record_time_key="DATETIME",
            record_tag_keys=["STATION_ID", "MAC", "GEOHASH"],
            record_field_keys=["PM25", "AQI", "TEMP", "HUM", "CO2"],
        )
        # print('Point:', point)
        yield point


def sync_ingesting_stations_measurements(
    station_measurements: pd.DataFrame, influxdb_version: str
) -> None: # pragma: no cover
    # How to use RxPY to prepare batches for synchronous write into InfluxDB
    # https://github.com/influxdata/influxdb-client-python/blob/master/examples/import_data_set_sync_batching.py
    #
    # InfluxDBClientSync
    #
    # Load Credentials
    project_path = Path.cwd()
    conf_path = str(project_path / settings.CONF_SOURCE)
    conf_loader = ConfigLoader(conf_source=conf_path, env="local")
    credentials = conf_loader.get("credentials*", "credentials*/**")
    # Check InfluxDB Version
    if influxdb_version == "2.x":
        # Secrets
        # You can generate an API token from the "API Tokens Tab" in the UI
        url = credentials["influxdb"]["url"]
        token = credentials["influxdb"]["token"]
        org = credentials["influxdb"]["org"]
        bucket = credentials["influxdb"]["bucket"]
    elif influxdb_version == "1.8":
        # Secrets
        url = credentials["influxdb"]["url"]
        username = credentials["influxdb"]["username"]
        password = credentials["influxdb"]["password"]
        token = f"{username}:{password}"
        database = credentials["influxdb"]["database"]
        retention_policy = "autogen"
        bucket = f"{database}/{retention_policy}"
        org = credentials["influxdb"]["org"]

    """
    Define Retry strategy - 3 attempts => 2, 4, 8
    """
    retries = WritesRetry(total=3, retry_interval=1, exponential_base=2)
    with InfluxDBClient(url=url, token=token, org=org, retries=retries) as client:

        """
        Use synchronous version of WriteApi to strongly depends on result of write
        """
        write_api = client.write_api(write_options=SYNCHRONOUS)

        """
        Prepare batches from generator
        """
        batches = rx.from_iterable(
            station_measurements_to_generator(station_measurements)
        ).pipe(ops.buffer_with_count(500))

        def write_batch(batch):
            """
            Synchronous write
            """
            print(f"Writing... {len(batch)}")
            write_api.write(bucket=bucket, record=batch)

        """
        Write batches
        """
        batches.subscribe(
            on_next=lambda batch: write_batch(batch),
            on_error=lambda ex: print(f"Unexpected error: {ex}"),
            on_completed=lambda: print("Import finished!"),
        )


def ingesting_influxdb(
    pm25_clean: pd.DataFrame,
    temp_raw: pd.DataFrame,
    hum_raw: pd.DataFrame,
    co2_raw: pd.DataFrame,
    aqi_instant: pd.DataFrame,
    tangara_stations: pd.DataFrame,
    influxdb_version: str,
) -> pd.DataFrame: # pragma: no cover
    """
        Ingesting each Tangara Sensor DataFrame Measurements to InfluxDB
        TODO: Refactoring Code to reach Test Coverage upper 90% please read and edit .coveragerc
    Args:
        pm25_clean: PM25 measurement
        temp_raw: Temperature measurement
        hum_raw: Humidity measurement
        co2_raw: CO2 measurement
        aqi_instant: AQI measurement
        tangara_stations: Tangara Stations
        influxdb_version: InfluxDB version
    Returns:
        stations_measurements: Tangara Stations Measurements
    """
    # Get Tangara Stations Measurements Dictionary
    stations_measurements = get_stations_measurements(
        tangara_stations, pm25_clean, aqi_instant, temp_raw, hum_raw, co2_raw
    )

    # Synchronous ingesting each Tangara Station Measurements
    for station_id, measurements in stations_measurements.items():
        print("Ingesting:", station_id, "...")
        sync_ingesting_stations_measurements(measurements, influxdb_version)

    return stations_measurements

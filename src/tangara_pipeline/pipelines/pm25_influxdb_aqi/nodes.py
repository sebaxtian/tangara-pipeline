"""
This is a boilerplate pipeline 'pm25_influxdb_aqi'
generated using Kedro 0.18.1
"""
import pandas as pd
from pathlib import Path
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from datetime import datetime
from influxdb_client import InfluxDBClient


def ingesting_influxdb(data_sensors: pd.DataFrame, measurement_name: str, influxdb_version: str) -> None: # pragma: no cover
    """
        Ingesting each Tangara Sensor DataFrame to InfluxDB
        TODO: Refactoring Code to reach Test Coverage upper 90%
    Args:
        data_sensors: Tangara Sensor DataFrame
    Returns:
        None
    """
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
    # Update Datatype
    data_sensors[data_sensors.columns.to_list()[1:]] = data_sensors[data_sensors.columns.to_list()[1:]].astype("float64")
    #print("data_sensors.dtypes: ", data_sensors.dtypes)
    # Ingesting each Tangara Sensor DataFrame to InfluxDB
    for column in data_sensors.columns[1:]:
        # For each Data Sensor
        tangara_X = data_sensors[["DATETIME", column]].copy()
        tangara_X["FIELD"] = column
        """
        Ingest DataFrame
        """
        # print()
        # print(f"=== Ingesting DataFrame {column} via batching API ===")
        # print()
        # startTime = datetime.now()

        with InfluxDBClient(url=url, token=token, org=org) as client:
            """
            Use batching API
            """
            with client.write_api() as write_api:
                write_api.write(
                    bucket=bucket,
                    record=tangara_X,
                    data_frame_timestamp_column="DATETIME",
                    data_frame_tag_columns=["FIELD"],
                    data_frame_measurement_name=measurement_name,
                    data_frame_timestamp_timezone="America/Bogota",
                )
                # print()
                # print(f"Wait to finishing ingesting DataFrame {column}...")
                # print()

        # print()
        # print(f'Import finished in: {datetime.now() - startTime}')
        # print()
        """
        Close client
        """
        client.close()

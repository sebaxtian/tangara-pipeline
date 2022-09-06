"""
This is a boilerplate pipeline 'pm25'
generated using Kedro 0.18.1
"""
from datetime import datetime, timedelta
import pandas as pd
import sys


# Drop Outliers
def _drop_outliers(data_sensors: pd.DataFrame) -> pd.DataFrame:
    # AQI Scale
    aqi_scale = (
        ("Good", 0, 50),
        ("Moderate", 51, 100),
        ("Unhealthy for Sensitive Groups", 101, 150),
        ("Unhealthy", 151, 200),
        ("Very Unhealthy", 201, 300),
        ("Harzardous", 301, sys.maxsize),
    )
    # Max AQI Scale Value
    max_aqi_value = aqi_scale[-1][1]

    # Each Data Sensor
    for column in data_sensors.columns:
        if column.find("TANGARA") != -1:
            # Apply mask to max value allowed, Max AQI Scale Value
            data_sensors[column] = data_sensors[column].mask(
                data_sensors[column] > max_aqi_value, max_aqi_value
            )

            # Standard Deviation
            std = data_sensors[column].std()
            # Range = max value - min value
            rango = data_sensors[column].max() - data_sensors[column].min()
            # Quantiles
            media = data_sensors[column].median()
            Q1 = data_sensors[column].quantile(q=0.25)
            Q2 = data_sensors[column].quantile(q=0.50)
            Q3 = data_sensors[column].quantile(q=0.75)
            min_val = data_sensors[column].quantile(q=0)
            max_val = data_sensors[column].quantile(q=1.0)
            # Interquartil Range
            iqr = Q3 - Q1
            # Limites para deteccion de outliers (solo aplica para datos simetricamente distribuidos)**
            min_limit = Q1 - 1.5 * iqr
            max_limit = Q3 + 1.5 * iqr
            # Medidas de Tendencia Central**
            str_resume = (
                f"{column}:\n"
                f"std: {std}\n"
                f"rango: {rango}\n"
                f"media: {media}\n"
                f"Q1: {Q1}\n"
                f"Q2: {Q2}\n"
                f"Q3: {Q3}\n"
                f"min_val: {min_val}\n"
                f"max_val: {max_val}\n"
                f"iqr: {iqr}\n"
                f"min_limit: {min_limit}\n"
                f"max_limit: {max_limit}\n\n"
            )
            # print(str_resume)

            # Drop Outliers, using max_limit to apply mask and drop outliers
            data_sensors[column] = data_sensors[column].mask(
                data_sensors[column] > max_limit, None
            )

    return data_sensors


# Add DateTime string values to Data Sensors
def _add_datetime_str_values(data_sensors: pd.DataFrame) -> pd.DataFrame:
    # Date value string
    data_sensors["DATE"] = data_sensors["DATETIME"].transform(
        lambda x: x.strftime("%x")
    )
    # Time value string
    data_sensors["TIME"] = data_sensors["DATETIME"].transform(
        lambda x: x.strftime("%T")
    )
    # Weekday value string
    data_sensors["WEEKDAY"] = data_sensors["DATETIME"].transform(
        lambda x: x.strftime("%A")
    )
    # Month value string
    data_sensors["MONTH"] = data_sensors["DATETIME"].transform(
        lambda x: x.strftime("%B")
    )
    # Year value string
    data_sensors["YEAR"] = data_sensors["DATETIME"].transform(
        lambda x: x.strftime("%Y")
    )

    return data_sensors


def pm25(raw_data_sensors: pd.DataFrame) -> pd.DataFrame:
    """
        Process Raw data for sensors registered by Tangara without outliers
    Args:
        raw_data_sensors: Raw data for sensors registered by Tangara
    Returns:
        PM25 data from Tangara sensors without outliers
    """
    # DateTime
    raw_data_sensors["DATETIME"] = pd.to_datetime(raw_data_sensors["DATETIME"])

    # Data Sensors with DateTime string values
    data_sensors = _add_datetime_str_values(raw_data_sensors)

    # Drop Outliers
    pm25 = _drop_outliers(data_sensors)

    return pm25


# Get pm25 resample mean by hour
def pm25_by_hour(pm25: pd.DataFrame) -> pd.DataFrame:
    """
        Process PM25 data sensors from Tangara sensors without outliers
    Args:
        pm25: PM25 data from Tangara sensors without outliers
    Returns:
        PM25 data by hour from Tangara sensors without outliers
    """
    # DateTime
    pm25["DATETIME"] = pd.to_datetime(pm25["DATETIME"])

    # Resample PM25 data by hour from Tangara sensors without outliers
    pm25_by_hour = (
        pm25.resample("H", on="DATETIME").mean().reset_index()
    )  # .median().reset_index()
    pm25_by_hour = _add_datetime_str_values(pm25_by_hour)

    return pm25_by_hour


# Get pm25 resample mean by movil 24h
def pm25_movil_24h(pm25_by_hour: pd.DataFrame) -> pd.DataFrame:
    """
        Process PM25 data by hour sensors from Tangara sensors without outliers
    Args:
        pm25_by_hour: PM25 data by hour from Tangara sensors without outliers
    Returns:
        PM25 data movil 24h from Tangara sensors without outliers
    """
    pm25_movil_24h = {}

    # DateTime
    pm25_by_hour["DATETIME"] = pd.to_datetime(pm25_by_hour["DATETIME"])

    # Resample PM25 data movil 24h from Tangara sensors without outliers
    for current_datetime in pm25_by_hour["DATETIME"].to_list():
        last_24h = current_datetime - timedelta(hours=24)
        pm25_last_24h = (
            pm25_by_hour[
                (pm25_by_hour["DATETIME"] <= current_datetime)
                & (pm25_by_hour["DATETIME"] > last_24h)
            ]
            .resample("D", on="DATETIME", origin="end")
            .mean()
            .reset_index()
        )
        pm25_movil_24h[current_datetime.isoformat()] = pm25_last_24h

    # Contact PM25 data movil 24h
    pm25_movil_24h = pd.concat(list(pm25_movil_24h.values())).reset_index(drop=True)
    pm25_movil_24h = _add_datetime_str_values(pm25_movil_24h)

    return pm25_movil_24h

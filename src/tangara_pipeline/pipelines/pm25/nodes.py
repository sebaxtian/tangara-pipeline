"""
This is a boilerplate pipeline 'pm25'
generated using Kedro 0.18.1
"""
from datetime import datetime, timedelta
import pandas as pd


# Get Date value string
def _get_date_str(timestamp: datetime.timestamp) -> str:
    return timestamp.strftime("%x")


# Get Time value string
def _get_time_str(timestamp: datetime.timestamp) -> str:
    return timestamp.strftime("%T")


# Get Weekday value string
def _get_weekday_str(timestamp: datetime.timestamp) -> str:
    return timestamp.strftime("%A")


# Get Month value string
def _get_month_str(timestamp: datetime.timestamp) -> str:
    return timestamp.strftime("%B")


# Get Year value string
def _get_year_str(timestamp: datetime.timestamp) -> str:
    return timestamp.strftime("%Y")


# Add DateTime string values to Data Sensors
def _add_datetime_str_values(raw_data_sensors: pd.DataFrame) -> pd.DataFrame:
    # Date value string
    raw_data_sensors["Date"] = raw_data_sensors["Timestamp"].transform(_get_date_str)
    # Time value string
    raw_data_sensors["Time"] = raw_data_sensors["Timestamp"].transform(_get_time_str)
    # Weekday value string
    raw_data_sensors["Weekday"] = raw_data_sensors["Timestamp"].transform(
        _get_weekday_str
    )
    # Month value string
    raw_data_sensors["Month"] = raw_data_sensors["Timestamp"].transform(_get_month_str)
    # Year value string
    raw_data_sensors["Year"] = raw_data_sensors["Timestamp"].transform(_get_year_str)

    return raw_data_sensors


def pm25(raw_data_sensors: pd.DataFrame) -> pd.DataFrame:
    """
        Process Raw data for sensors registered by Tangara and include on it
        string DateTime values.
    Args:
        raw_data_sensors: Raw data for sensors registered by Tangara
    Returns:
        Data for sensors registered by Tangara and it's DateTime string values.
    """
    # Timestamp
    raw_data_sensors["Timestamp"] = pd.to_datetime(raw_data_sensors["Datetime"])

    # Data Sensors with DateTime string values
    data_sensors = _add_datetime_str_values(raw_data_sensors)

    return data_sensors


def resample_pm25_by_hour(pm25_data_sensors: pd.DataFrame) -> pd.DataFrame:
    """
        Resample pm25 data mean sensors by hour registered by Tangara and include on it
        string DateTime values.
    Args:
        pm25_data_sensors: pm25 data for sensors registered by Tangara
    Returns:
        Resample pm25 data mean by hour for sensors registered by Tangara and it's DateTime string values.
    """
    # Timestamp
    pm25_data_sensors["Timestamp"] = pd.to_datetime(pm25_data_sensors["Datetime"])

    # Resample mean by Hour
    pm25_by_hour = pm25_data_sensors.resample("H", on="Timestamp").mean().reset_index()

    # Data Sensors with DateTime string values
    pm25_by_hour = _add_datetime_str_values(pm25_by_hour)

    return pm25_by_hour


def resample_pm25_movil_24h(pm25_by_hour: pd.DataFrame) -> pd.DataFrame:
    """
        Resample pm25 data mean sensors by movil 24h registered by Tangara and include on it
        string DateTime values.
    Args:
        pm25_by_hour: pm25 data by hour for sensors registered by Tangara
    Returns:
        Resample pm25 data mean by movil 24h for sensors registered by Tangara and it's DateTime string values.
    """
    # Timestamp
    pm25_by_hour["Timestamp"] = pd.to_datetime(pm25_by_hour["Timestamp"])

    pm25_movil_24h = {}
    # For each datetime
    for current_datetime in pm25_by_hour["Timestamp"].to_list():
        last_24h = current_datetime - timedelta(hours=24)
        # Filter by last 24h from current datetime and resample mean on it
        pm25_last_24h = (
            pm25_by_hour[
                (pm25_by_hour["Timestamp"] <= current_datetime)
                & (pm25_by_hour["Timestamp"] > last_24h)
            ]
            .resample("D", on="Timestamp", origin="end")
            .mean()
            .reset_index()
        )
        pm25_movil_24h[current_datetime.isoformat()] = pm25_last_24h

    # Resample pm25 mean movil 24h
    pm25_movil_24h = pd.concat(list(pm25_movil_24h.values())).reset_index(drop=True)
    pm25_movil_24h = _add_datetime_str_values(pm25_movil_24h)

    return pm25_movil_24h

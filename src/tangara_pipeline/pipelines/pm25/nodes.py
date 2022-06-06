"""
This is a boilerplate pipeline 'pm25'
generated using Kedro 0.18.1
"""
from datetime import datetime
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

"""
This is a boilerplate pipeline 'raw_data_sensors'
generated using Kedro 0.18.1
"""
import pandas as pd
from tangara_pipeline.pipelines.raw_data_from_csv.nodes import raw_data_sensors as raw_data_sensors_csv
from tangara_pipeline.pipelines.raw_data_from_api.nodes import raw_data_sensors as raw_data_sensors_api


def raw_data_sensors(tangaras: pd.DataFrame, spreadsheets: pd.DataFrame, start_datetime: str, end_datetime: str, raw_data_origin: str) -> pd.DataFrame:
    
    if raw_data_origin == 'csv':
        return raw_data_sensors_csv(tangaras, spreadsheets)
    elif raw_data_origin == 'api':
        return raw_data_sensors_api(tangaras, start_datetime, end_datetime)
    
    return pd.DataFrame()

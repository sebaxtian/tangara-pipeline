"""
This is a boilerplate pipeline 'raw_data_from_api'
generated using Kedro 0.18.1
"""
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta


# Get Period Time
def _get_period_time(start_datetime: str, end_datetime: str) -> str:
    start_datetime = int(datetime.fromisoformat(start_datetime).timestamp() * 1000)
    end_datetime = int(datetime.fromisoformat(end_datetime).timestamp() * 1000)
    return f"time >= {start_datetime}ms and time <= {end_datetime}ms"


# Get SQL Query Sensors
def _get_sql_query_sensors(tangaras: pd.DataFrame, start_datetime: str, end_datetime: str) -> str:
    sql_query = ""
    period_time = _get_period_time(start_datetime, end_datetime)
    #period_time = "time >= now() - 1h and time <= now()"
    for mac in tangaras['MAC'].to_list():
        sql_query += "SELECT \"name\", last(\"pm25\") "\
                    "FROM \"fixed_stations_01\" WHERE "\
                    f"(\"name\" = '{mac}') AND "\
                    f"{period_time} " \
                    "GROUP BY time(30s) fill(none); "
    return sql_query[:-2]


# Request to InfluxDB API REST
def _request_to_influxdb(sql_query: str) -> requests.Response:
    endpoint = "http://influxdb.canair.io:8086/query"
    database = "canairio"
    parameters = {
        'db': database,
        'q': sql_query,
        'epoch': 'ms'
    }
    return requests.get(endpoint, params=parameters)


# Get Raw Data Sensors
def raw_data_sensors(tangaras: pd.DataFrame, start_datetime: str, end_datetime: str) -> pd.DataFrame:
    """Get from InfluxDB API REST the data for sensors registered by Tangara.
       Use the parameter to define the period time used by the request.

    Args:
        tangaras: Raw data for each sensor registered by Tangara
    Returns:
        Raw data for sensors registered by Tangara.
    """
    df_sensors = []

    sql_query = _get_sql_query_sensors(tangaras, start_datetime, end_datetime)
    influxdb_api_request = _request_to_influxdb(sql_query)

    result_list = influxdb_api_request.json()['results']
    result_list = [value for value in result_list if 'series' in value]
    df_influxdb_sensors = pd.json_normalize(result_list, record_path=['series', 'values']).sort_values(by=[0])
    
    for index, row in tangaras.iterrows():
        df_sensor = df_influxdb_sensors.loc[df_influxdb_sensors[1] == row['MAC']].reset_index(drop=True)[[0, 2]] # Warning
        if not df_sensor.empty:
            df_sensor.rename(columns={0: 'Datetime', 2: row['Label_ID']}, inplace=True)
            df_sensor.set_index('Datetime', inplace=True)
            df_sensors.append(df_sensor)
    
    df_sensors = df_sensors[0].join(df_sensors[1:]).reset_index()

    tz = timezone(timedelta(hours=-5))
    df_sensors['Datetime'] = df_sensors['Datetime'].apply(lambda x: datetime.fromtimestamp(x / 1000, tz=tz).isoformat())

    df_sensors[df_sensors.columns.to_list()[1:]] = df_sensors[df_sensors.columns.to_list()[1:]].astype('Int64')
    
    return df_sensors

"""
This is a boilerplate pipeline 'tangara_stations'
generated using Kedro 0.18.1
"""
from typing import List
import pandas as pd
from datetime import datetime, timezone, timedelta
import requests
from io import StringIO
import geohash2



# Get start and nowcast Timestamp
def get_start_nowcast_timestamp(nowcast_datetime: str, start_datetime: str = None) -> List[int]:
    #
    # Please nowcast_datetime must be: NOWCAST_DATETIME=$(TZ='America/Bogota' date '+%Y-%m-%dT%H:%M:%S')
    # Check the script bash: run.sh
    #

    # NowCast DateTime
    nowcast_datetime = datetime.fromisoformat(nowcast_datetime)
    print('-------------nowcast_datetime-------------->>>> ', nowcast_datetime)

    # Start DateTime
    if not start_datetime:
        # Start DateTime last 24 hours
        start_datetime = datetime.fromisoformat((nowcast_datetime - timedelta(hours=24)).isoformat())
    else:
        # Start DateTime
        start_datetime = datetime.fromisoformat(start_datetime)
    print('-------------start_datetime-------------->>>> ', start_datetime)

    # Current DateTime
    nowcast_timestamp = int(nowcast_datetime.timestamp() * 1000)
    # Start DateTime
    start_timestamp = int(start_datetime.timestamp() * 1000)

    print('-------------nowcast_timestamp-------------->>>> ', nowcast_timestamp)
    print('-------------start_timestamp-------------->>>> ', start_timestamp)

    return [start_timestamp, nowcast_timestamp]


# Request to InfluxDB API REST
def request_to_influxdb(sql_query: str) -> str:
    endpoint = "http://influxdb.canair.io:8086/query"
    database = "canairio"
    parameters = {
        'db': database,
        'q': sql_query,
        'epoch': 'ms'
    }
    # To get response as CSV text
    headers = {'Accept': 'application/csv'}
    # GET Request
    return requests.get(endpoint, params=parameters, headers=headers)


# Get SQL Query Tangara Stations
def get_sql_query_tangara_stations(start_datetime: int, end_datetime: int) -> str:
    sql_query = ""
    # Period DateTime
    period_time = f"time >= {start_datetime}ms AND time <= {end_datetime}ms"
    # SQL
    sql_query = "SELECT DISTINCT(geo) AS \"geohash\" "\
                "FROM \"fixed_stations_01\" WHERE "\
                    "(\"geo3\" = 'd29') AND "\
                    f"{period_time} "\
                "GROUP BY \"name\";"
    return sql_query


# Get SQL Query Data Measurement
def get_sql_query_measurement(tangaras: pd.DataFrame, measurement: str, start_datetime: int, end_datetime: int) -> str:
    sql_query = ""
    # Period DateTime
    period_time = f"time >= {start_datetime}ms AND time <= {end_datetime}ms"
    # SQL
    for mac in tangaras['MAC'].to_list():
        sql_query += "SELECT \"name\", "\
                    f"last(\"{measurement}\") "\
                    "FROM \"fixed_stations_01\" WHERE "\
                    f"(\"name\" = '{mac}') AND "\
                    f"{period_time} " \
                    "GROUP BY time(30s) fill(none); "
    return sql_query[:-2]


# Get Data Frame Tangara Stations
def get_tangara_stations(start_datetime: int, end_datetime: int) -> pd.DataFrame:
    # SQL Query Tangaras
    sql_query = get_sql_query_tangara_stations(start_datetime, end_datetime)
    # InfluxDB API REST Request
    influxdb_api_request = request_to_influxdb(sql_query)
    #print(influxdb_api_request)
    tangara_stations = pd.read_csv(StringIO(influxdb_api_request.text), sep=",")
    
    # Remove/Add Columns
    tangara_stations = tangara_stations[['tags', 'geohash']]
    tangara_stations['MAC'] = tangara_stations['tags'].apply(lambda x: x.split('=')[1])
    tangara_stations['GEOLOCATION'] = tangara_stations['geohash'].apply(lambda x: " ".join(f'{value:.8f}' for value in list(geohash2.decode_exactly(x)[0:2])))
    tangara_stations['LATITUDE'] = tangara_stations['GEOLOCATION'].apply(lambda x: x.split(' ')[0])
    tangara_stations['LONGITUDE'] = tangara_stations['GEOLOCATION'].apply(lambda x: x.split(' ')[1])
    tangara_stations['tags'] = tangara_stations['tags'].apply(lambda x: f"TANGARA_{x[-4:]}")
    tangara_stations.rename(columns={'tags': 'ID', 'geohash': 'GEOHASH'}, inplace=True)

    # Date time when query is executed
    tz = timezone(timedelta(hours=-5))
    tangara_stations['DATETIME'] = datetime.now(tz=tz).isoformat()

    # Reorder Columns
    tangara_stations = tangara_stations[['DATETIME', 'ID', 'MAC', 'GEOHASH', 'GEOLOCATION', 'LATITUDE', 'LONGITUDE']]
    
    # Update dtype
    tangara_stations[['LATITUDE', 'LONGITUDE']] = tangara_stations[['LATITUDE', 'LONGITUDE']].astype('float64')

    return tangara_stations


# Get Data Frame Measurement
def get_df_measurement(tangaras: pd.DataFrame, measurement: str, start_datetime: int, end_datetime: int) -> pd.DataFrame:
    # Data Frame Sensors
    df_sensors = []
    # SQL Query Data Sensors
    sql_query = get_sql_query_measurement(tangaras, measurement, start_datetime, end_datetime)
    # InfluxDB API REST Request
    influxdb_api_request = request_to_influxdb(sql_query)
    #print(influxdb_api_request)
    df_influxdb_api_sensors = pd.read_csv(StringIO(influxdb_api_request.text), sep=",")

    # Remove/Add Columns
    df_influxdb_api_sensors = df_influxdb_api_sensors[['time', 'name.1', 'last']]
    df_influxdb_api_sensors.rename(columns={'time': 'DATETIME', 'name.1': 'MAC', 'last': measurement.upper()}, inplace=True)

    # Truncate Response
    for index, row in tangaras.iterrows():
        df_sensor = df_influxdb_api_sensors.loc[df_influxdb_api_sensors['MAC'] == row['MAC']].reset_index(drop=True)[['DATETIME', measurement.upper()]] # Warning
        if not df_sensor.empty:
            df_sensor.rename(columns={measurement.upper(): row['ID']}, inplace=True)
            df_sensor.set_index('DATETIME', inplace=True)
            df_sensors.append(df_sensor)
    
    # Join Data Frames
    df_sensors = df_sensors[0].join(df_sensors[1:]).reset_index()

    # Update datetime
    tz = timezone(timedelta(hours=-5))
    df_sensors['DATETIME'] = df_sensors['DATETIME'].apply(lambda x: datetime.fromtimestamp(int(x) / 1000, tz=tz).isoformat())
    
    # Update dtype
    df_sensors[df_sensors.columns.to_list()[1:]] = df_sensors[df_sensors.columns.to_list()[1:]].astype('float64')
    
    return df_sensors


def tangara_stations(nowcast_datetime: str) -> pd.DataFrame:
    """
        Request tangara stations via API from InfluxDB for registered Tangara stations
    Args:
        nowcast_datetime: Datetime to caculate the last 24 hours
    Returns:
        tangara_stations: Registered Tangara stations
    """
    # start and nowcast Timestamp
    start_timestamp, nowcast_timestamp = get_start_nowcast_timestamp(nowcast_datetime)
    print("------------", start_timestamp, nowcast_timestamp)
    # Data Frame Tangara Stations
    return get_tangara_stations(start_timestamp, nowcast_timestamp)


def pm25_raw(tangara_stations: pd.DataFrame, nowcast_datetime: str) -> pd.DataFrame:
    """
        Request pm25 measurement via API from InfluxDB for registered Tangara stations
    Args:
        tangara_stations: Registered Tangara stations
        nowcast_datetime: Datetime to caculate the last 24 hours
    Returns:
        pm25_raw: PM25 measurement for registered Tangara stations
    """
    # Start and NowCast Timestamp
    start_timestamp, nowcast_timestamp = get_start_nowcast_timestamp(nowcast_datetime)
    # Data Frame PM25 Raw
    return get_df_measurement(tangara_stations, 'pm25', start_timestamp, nowcast_timestamp)


def temp_raw(tangara_stations: pd.DataFrame, nowcast_datetime: str) -> pd.DataFrame:
    """
        Request temp measurement via API from InfluxDB for registered Tangara stations
    Args:
        tangara_stations: Registered Tangara stations
        nowcast_datetime: Datetime to caculate the last 24 hours
    Returns:
        temp_raw: Temperature measurement for registered Tangara stations
    """
    # Start and NowCast Timestamp
    start_timestamp, nowcast_timestamp = get_start_nowcast_timestamp(nowcast_datetime)
    # Data Frame Temp Raw
    return get_df_measurement(tangara_stations, 'tmp', start_timestamp, nowcast_timestamp)


def hum_raw(tangara_stations: pd.DataFrame, nowcast_datetime: str) -> pd.DataFrame:
    """
        Request hum measurement via API from InfluxDB for registered Tangara stations
    Args:
        tangara_stations: Registered Tangara stations
        nowcast_datetime: Datetime to caculate the last 24 hours
    Returns:
        hum_raw: Humidity measurement for registered Tangara stations
    """
    # Start and NowCast Timestamp
    start_timestamp, nowcast_timestamp = get_start_nowcast_timestamp(nowcast_datetime)
    # Data Frame Hum Raw
    return get_df_measurement(tangara_stations, 'hum', start_timestamp, nowcast_timestamp)


def co2_raw(tangara_stations: pd.DataFrame, nowcast_datetime: str) -> pd.DataFrame:
    """
        Request co2 measurement via API from InfluxDB for registered Tangara stations
    Args:
        tangara_stations: Registered Tangara stations
        nowcast_datetime: Datetime to caculate the last 24 hours
    Returns:
        co2_raw: CO2 measurement for registered Tangara stations
    """
    # Start and NowCast Timestamp
    start_timestamp, nowcast_timestamp = get_start_nowcast_timestamp(nowcast_datetime)
    # Data Frame CO2 Raw
    return get_df_measurement(tangara_stations, 'co2', start_timestamp, nowcast_timestamp)

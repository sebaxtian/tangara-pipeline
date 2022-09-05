"""
This is a boilerplate pipeline 'raw_data_from_api'
generated using Kedro 0.18.1
"""
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta
from io import StringIO
import geohash2


# Request to InfluxDB API REST
def _request_to_influxdb(sql_query: str) -> requests.Response:
    endpoint = "http://influxdb.canair.io:8086/query"
    database = "canairio"
    parameters = {
        'db': database,
        'q': sql_query,
        'epoch': 'ms'
    }
    # To get response as CSV text
    headers = {'Accept': 'application/csv'}
    # GET Request, Response
    return requests.get(endpoint, params=parameters, headers=headers)


# Get Period Time
def _get_period_time(start_datetime: str, end_datetime: str) -> str:
    start_datetime = int(datetime.fromisoformat(start_datetime).timestamp() * 1000)
    end_datetime = int(datetime.fromisoformat(end_datetime).timestamp() * 1000)
    return f"time >= {start_datetime}ms and time <= {end_datetime}ms"


# Get SQL Query Tangaras
def _get_sql_query_tangaras(start_datetime: str, end_datetime: str) -> str:
    sql_query = ""
    #period_time = "time >= now() - 1h and time <= now()"
    #period_time = "time >= now() - 24h and time <= now()"
    period_time = _get_period_time(start_datetime, end_datetime)
    sql_query = "SELECT DISTINCT(geo) AS \"geohash\" "\
                "FROM \"fixed_stations_01\" WHERE "\
                    "(\"geo3\" = 'd29') AND "\
                    f"{period_time} "\
                "GROUP BY \"name\";"
    return sql_query


# Get Tangaras
def tangaras(start_datetime: str, end_datetime: str) -> pd.DataFrame:
    """Get from InfluxDB API REST the data for sensors registered by Tangara.
       Use the parameter to define the period time used by the request.

    Args:
        start_datetime: From datetime when the data were be collected
        end_datetime: To datetime when the data were be collected
    Returns:
        Tangara sensors that were registered.
    """
    # SQL Query Tangaras
    sql_query = _get_sql_query_tangaras(start_datetime, end_datetime)
    # InfluxDB API REST Request
    influxdb_api_request = _request_to_influxdb(sql_query)
    df_tangaras = pd.read_csv(StringIO(influxdb_api_request.text), sep=",")
    
    # Remove/Add Columns
    df_tangaras = df_tangaras[['tags', 'geohash']]
    df_tangaras['MAC'] = df_tangaras['tags'].apply(lambda x: x.split('=')[1])
    df_tangaras['GEOLOCATION'] = df_tangaras['geohash'].apply(lambda x: " ".join(str(value) for value in list(geohash2.decode_exactly(x)[0:2])))
    df_tangaras['LATITUDE'] = df_tangaras['GEOLOCATION'].apply(lambda x: x.split(' ')[0])
    df_tangaras['LONGITUDE'] = df_tangaras['GEOLOCATION'].apply(lambda x: x.split(' ')[1])
    df_tangaras['tags'] = df_tangaras['tags'].apply(lambda x: f"TANGARA_{x[-4:]}")
    df_tangaras.rename(columns={'tags': 'ID', 'geohash': 'GEOHASH'}, inplace=True)
    # Date time when query is executed
    tz = timezone(timedelta(hours=-5))
    df_tangaras['DATETIME'] = datetime.now(tz=tz)

    return df_tangaras


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


# Get Raw Data Sensors
def raw_data_sensors(tangaras: pd.DataFrame, start_datetime: str, end_datetime: str) -> pd.DataFrame:
    """Get from InfluxDB API REST the data for sensors registered by Tangara.
       Use the parameter to define the period time used by the request.

    Args:
        tangaras: Raw data for each sensor registered by Tangara
        start_datetime: From datetime when the data were be collected
        end_datetime: To datetime when the data were be collected
    Returns:
        Raw data for sensors registered by Tangara.
    """
    # Data Frame Sensors
    df_sensors = []
    # SQL Query Data Sensors
    sql_query = _get_sql_query_sensors(tangaras, start_datetime, end_datetime)
    # InfluxDB API REST Request
    influxdb_api_request = _request_to_influxdb(sql_query)
    df_influxdb_api_sensors = pd.read_csv(StringIO(influxdb_api_request.text), sep=",")

    # Remove/Add Columns
    df_influxdb_api_sensors = df_influxdb_api_sensors[['time', 'name.1', 'last']]
    df_influxdb_api_sensors.rename(columns={'time': 'DATETIME', 'name.1': 'MAC', 'last': 'PM25'}, inplace=True)

    # Truncate Response
    for index, row in tangaras.iterrows():
        df_sensor = df_influxdb_api_sensors.loc[df_influxdb_api_sensors['MAC'] == row['MAC']].reset_index(drop=True)[['DATETIME', 'PM25']] # Warning
        if not df_sensor.empty:
            df_sensor.rename(columns={'PM25': row['ID']}, inplace=True)
            df_sensor.set_index('DATETIME', inplace=True)
            df_sensors.append(df_sensor)
    
    # Join All Dataframes
    df_sensors = df_sensors[0].join(df_sensors[1:]).reset_index()

    # Date Time ISO Format
    tz = timezone(timedelta(hours=-5))
    df_sensors['DATETIME'] = df_sensors['DATETIME'].apply(lambda x: datetime.fromtimestamp(int(x) / 1000, tz=tz).isoformat())

    # Update Columns Data Types
    df_sensors[df_sensors.columns.to_list()[1:]] = df_sensors[df_sensors.columns.to_list()[1:]].astype('Int64')
    
    return df_sensors

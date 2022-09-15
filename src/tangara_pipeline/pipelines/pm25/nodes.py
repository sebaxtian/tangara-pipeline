"""
This is a boilerplate pipeline 'pm25'
generated using Kedro 0.18.1
"""

from typing import List
import pandas as pd
import requests
from io import StringIO
from datetime import datetime, timezone, timedelta
import geohash2



# Request to InfluxDB API REST
def request_to_influxdb(sql_query: str) -> requests.Response:
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


# Get SQL Query Tangaras
def get_sql_query_tangaras(start_datetime: int, end_datetime: int) -> str:
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


# Get Data Frame Tangaras
def get_df_tangaras(start_datetime: int, end_datetime: int) -> pd.DataFrame:
    # SQL Query Tangaras
    sql_query = get_sql_query_tangaras(start_datetime, end_datetime)
    # InfluxDB API REST Request
    influxdb_api_request = request_to_influxdb(sql_query)
    #print(influxdb_api_request)
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


# Get SQL Query Data Sensors
def get_sql_query_sensors(tangaras: pd.DataFrame, start_datetime: int, end_datetime: int) -> str:
    sql_query = ""
    # Period DateTime
    period_time = f"time >= {start_datetime}ms AND time <= {end_datetime}ms"
    # SQL
    for mac in tangaras['MAC'].to_list():
        sql_query += "SELECT \"name\", last(\"pm25\") "\
                    "FROM \"fixed_stations_01\" WHERE "\
                    f"(\"name\" = '{mac}') AND "\
                    f"{period_time} " \
                    "GROUP BY time(30s) fill(none); "
    return sql_query[:-2]


# Get Data Frame Sensors
def get_df_sensors(tangaras: pd.DataFrame, start_datetime: int, end_datetime: int) -> pd.DataFrame:
    # Data Frame Sensors
    df_sensors = []
    # SQL Query Data Sensors
    sql_query = get_sql_query_sensors(tangaras, start_datetime, end_datetime)
    # InfluxDB API REST Request
    influxdb_api_request = request_to_influxdb(sql_query)
    #print(influxdb_api_request)
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
    
    df_sensors = df_sensors[0].join(df_sensors[1:]).reset_index()

    df_sensors['DATETIME'] = df_sensors['DATETIME'].apply(lambda x: datetime.fromtimestamp(int(x) / 1000).isoformat())

    df_sensors[df_sensors.columns.to_list()[1:]] = df_sensors[df_sensors.columns.to_list()[1:]].astype('Int64')
    
    return df_sensors


def pm25_raw(nowcast_datetime: str) -> List[pd.DataFrame]:
    """
        Request raw data sensors via API from InfluxDB and then create
        the initial raw data for registered Tangara sensors
    Args:
        None
    Returns:
        Both pm25_raw and tangaras raw data for registered Tangara sensors
    """

    #
    # Please nowcast_datetime must be: NOWCAST_DATETIME=$(TZ='America/Bogota' date '+%Y-%m-%dT%H:%M:%S')
    # Check the script bash: run.sh
    #

    # NowCast DateTime
    nowcast_datetime = datetime.fromisoformat(nowcast_datetime)
    print('-------------nowcast_datetime-------------->>>> ', nowcast_datetime)

    # Start DateTime
    start_datetime = datetime.fromisoformat((nowcast_datetime - timedelta(hours=24)).isoformat())
    print('-------------start_datetime-------------->>>> ', start_datetime)

    # Current DateTime
    nowcast_timestamp = int(nowcast_datetime.timestamp() * 1000)
    # Start DateTime
    start_timestamp = int(start_datetime.timestamp() * 1000)

    print('-------------nowcast_timestamp-------------->>>> ', nowcast_timestamp)
    print('-------------start_timestamp-------------->>>> ', start_timestamp)

    # Data Frame Tangaras
    tangaras = get_df_tangaras(start_timestamp, nowcast_timestamp)

    # Data Frame Sensors
    pm25_raw = get_df_sensors(tangaras, start_timestamp, nowcast_timestamp)

    return [tangaras, pm25_raw]


# Drop Outliers
def drop_outliers(data_sensors: pd.DataFrame) -> pd.DataFrame:
    # Please check the Notebook pm25_nowcast_aqi.ipynb to refer information about this value.
    # Max PM25 Value
    MAX_PM25_VALUE = 500.5

    # Each Data Sensor
    for column in data_sensors.columns:
        if column != 'DATETIME':
            # Apply mask to max value allowed, Max AQI Scale Value
            data_sensors[column] = data_sensors[column].mask(data_sensors[column] > MAX_PM25_VALUE, MAX_PM25_VALUE)
            
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
            str_resume = f"{column}:\n"\
                        f"std: {std}\n"\
                        f"rango: {rango}\n"\
                        f"media: {media}\n"\
                        f"Q1: {Q1}\n"\
                        f"Q2: {Q2}\n"\
                        f"Q3: {Q3}\n"\
                        f"min_val: {min_val}\n"\
                        f"max_val: {max_val}\n"\
                        f"iqr: {iqr}\n"\
                        f"min_limit: {min_limit}\n"\
                        f"max_limit: {max_limit}\n\n"
            #print(str_resume)

            # Drop Outliers, using max_limit to apply mask and drop outliers
            data_sensors[column] = data_sensors[column].mask(data_sensors[column] > max_limit, None)
    
    return data_sensors


def pm25_clean(pm25_raw: pd.DataFrame) -> pd.DataFrame:
    """
        From pm25_raw data sensors drop outliers using PM25 Breakpoints Values,
        Please check the Notebook pm25_nowcast_aqi.ipynb to refer information about this
    Args:
        pm25_raw: raw data for registered Tangara sensors
    Returns:
        pm25_clean: clean data for registered Tangara sensors without outliers
    """
    # Data Frame Sensors
    return drop_outliers(pm25_raw)


# Get pm25 resample mean last hour
def resample_pm25_last_hour(data_sensors: pd.DataFrame) -> pd.DataFrame:
    # Timestamp Format
    data_sensors['DATETIME'] = pd.to_datetime(data_sensors['DATETIME'])
    return data_sensors.resample('H', on='DATETIME', origin='end').mean().reset_index()#.median().reset_index()


def pm25_last_hour(pm25_clean: pd.DataFrame) -> pd.DataFrame:
    """
        From pm25_clean data sensors resample PM25 date time values to last hour
    Args:
        pm25_clean: clean data for registered Tangara sensors
    Returns:
        pm25_last_hour: resample data for registered Tangara sensors to last hour
    """
    # Data Frame Sensors
    return resample_pm25_last_hour(pm25_clean)


# Get pm25 resample mean last 8 hours
def resample_pm25_last_8h(data_sensors: pd.DataFrame) -> pd.DataFrame:
    # Timestamp Format
    data_sensors['DATETIME'] = pd.to_datetime(data_sensors['DATETIME'])
    return data_sensors.resample('8H', on='DATETIME', origin='end').mean().reset_index()#.median().reset_index()


def pm25_last_8h(pm25_last_hour: pd.DataFrame) -> pd.DataFrame:
    """
        From pm25_last_hour data sensors resample PM25 date time values to last 8 hours
    Args:
        pm25_last_hour: last hour data for registered Tangara sensors
    Returns:
        pm25_last_8h: resample data for registered Tangara sensors to last 8 hours
    """
    # Data Frame Sensors
    return resample_pm25_last_8h(pm25_last_hour)


# Get pm25 resample mean last 12 hours
def resample_pm25_last_12h(data_sensors: pd.DataFrame) -> pd.DataFrame:
    # Timestamp Format
    data_sensors['DATETIME'] = pd.to_datetime(data_sensors['DATETIME'])
    return data_sensors.resample('12H', on='DATETIME', origin='end').mean().reset_index()#.median().reset_index()


def pm25_last_12h(pm25_last_hour: pd.DataFrame) -> pd.DataFrame:
    """
        From pm25_last_hour data sensors resample PM25 date time values to last 12 hours
    Args:
        pm25_last_hour: last hour data for registered Tangara sensors
    Returns:
        pm25_last_12h: resample data for registered Tangara sensors to last 12 hours
    """
    # Data Frame Sensors
    return resample_pm25_last_12h(pm25_last_hour)


# Get pm25 resample mean last 24 hours
def resample_pm25_last_24h(data_sensors: pd.DataFrame) -> pd.DataFrame:
    # Timestamp Format
    data_sensors['DATETIME'] = pd.to_datetime(data_sensors['DATETIME'])
    return data_sensors.resample('24H', on='DATETIME', origin='end').mean().reset_index()#.median().reset_index()


def pm25_last_24h(pm25_last_hour: pd.DataFrame) -> pd.DataFrame:
    """
        From pm25_last_hour data sensors resample PM25 date time values to last 24 hours
    Args:
        pm25_last_hour: last hour data for registered Tangara sensors
    Returns:
        pm25_last_24h: resample data for registered Tangara sensors to last 24 hours
    """
    # Data Frame Sensors
    return resample_pm25_last_24h(pm25_last_hour)

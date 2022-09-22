"""
This is a boilerplate pipeline 'pm25'
generated using Kedro 0.18.1
"""

import pandas as pd



# Drop PM25 Outliers
def drop_pm25_outliers(data_sensors: pd.DataFrame) -> pd.DataFrame:
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

            # Drop PM25 Outliers, using max_limit to apply mask and drop PM25 outliers
            data_sensors[column] = data_sensors[column].mask(data_sensors[column] > max_limit, None)
    
    return data_sensors


def pm25_clean(pm25_raw: pd.DataFrame) -> pd.DataFrame:
    """
        From pm25_raw data sensors drop PM25 outliers using PM25 Breakpoints Values,
        Please check the Notebook pm25_nowcast_aqi.ipynb to refer information about this
    Args:
        pm25_raw: raw data for registered Tangara sensors
    Returns:
        pm25_clean: clean data for registered Tangara sensors without PM25 outliers
    """
    # Data Frame Sensors
    return drop_pm25_outliers(pm25_raw)


# Get pm25 resample mean last hour
def resample_pm25_last_hour(data_sensors: pd.DataFrame) -> pd.DataFrame:
    # Timestamp Format
    data_sensors['DATETIME'] = pd.to_datetime(data_sensors['DATETIME'])
    data_sensors = data_sensors.resample('H', on='DATETIME', origin='end').mean().reset_index()#.median().reset_index()
    data_sensors['DATETIME'] = data_sensors['DATETIME'].apply(lambda x: x.isoformat())
    return data_sensors


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
    data_sensors = data_sensors.resample('8H', on='DATETIME', origin='end').mean().reset_index()#.median().reset_index()
    data_sensors['DATETIME'] = data_sensors['DATETIME'].apply(lambda x: x.isoformat())
    return data_sensors


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
    data_sensors = data_sensors.resample('12H', on='DATETIME', origin='end').mean().reset_index()#.median().reset_index()
    data_sensors['DATETIME'] = data_sensors['DATETIME'].apply(lambda x: x.isoformat())
    return data_sensors


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
    data_sensors = data_sensors.resample('24H', on='DATETIME', origin='end').mean().reset_index()#.median().reset_index()
    data_sensors['DATETIME'] = data_sensors['DATETIME'].apply(lambda x: x.isoformat())
    return data_sensors


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

"""
This is a boilerplate pipeline 'raw_data_from_csv'
generated using Kedro 0.18.1
"""
import pandas as pd
import re


def _convert_gsheets_url(url: str) -> str:
    try:
        worksheet_id = url.split('#gid=')[1]
    except:
        # Couldn't get worksheet id. Ignore it
        worksheet_id = None
    url = re.findall('https://docs.google.com/spreadsheets/d/.*?/',url)[0]
    url += 'export'
    url += '?format=csv'
    if worksheet_id:
        url += '&gid={}'.format(worksheet_id) # pragma: no cover
    return url


def raw_data_sensors(tangaras: pd.DataFrame, spreadsheets: pd.DataFrame) -> pd.DataFrame:
    """Get from spreadsheets the data for sensors registered by Tangara

    Args:
        tangaras: Raw data for each sensor registered by Tangara
        spreadsheets: Raw data for spreadsheets with sensor's data
    Returns:
        Raw data for sensors registered by Tangara.
    """
    df_sensors = {}
    sensors_label = tangaras['Label_ID'].to_list()

    for index, row in spreadsheets.iterrows():
        try:
            url = _convert_gsheets_url(row['URL'])
            df = pd.read_csv(url)
            df = df.filter(items=['Time'] + sensors_label)
            df_sensors[row['Name']] = df
            print('From', row['Name'], 'read successfully')
        except Exception: # pragma: no cover
            print('Could not read any data from', row['ID'], row['Name'], row['URL']) # pragma: no cover
    
    df_sensors = pd.concat(list(df_sensors.values()))
    df_sensors.rename(columns={'Time':'Datetime'}, inplace=True)
    
    df_sensors['Datetime'] = pd.to_datetime(df_sensors['Datetime'])
    df_sensors['Datetime'] = df_sensors['Datetime'].apply(lambda x: x.isoformat()+"-05:00")

    df_sensors[df_sensors.columns.to_list()[1:]] = df_sensors[df_sensors.columns.to_list()[1:]].astype('Int64')

    return df_sensors

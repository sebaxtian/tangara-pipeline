"""
This fixture contains the definition of aqi_last_hour_fixture DataFrame used by Nodes and Pipelines.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
import random


def get_random_data_sensors():
    random_data_sensors = []
    date_time_range = pd.date_range(start='2022-09-05T13:35:00', end='2022-09-06T13:35:00', freq='1H', tz='America/Bogota')
    tz = timezone(timedelta(hours=-5))
    for date_time_value in date_time_range:
        data_sensor = [
            datetime.fromtimestamp(int(date_time_value.timestamp()), tz=tz).isoformat(),
            random.randint(3, 301),
            random.randint(3, 301),
            random.randint(3, 301),
            random.randint(3, 301),
            random.randint(3, 301),
            random.randint(3, 301),
            random.randint(3, 301),
        ]
        random_data_sensors.append(data_sensor)
    return random_data_sensors


@pytest.fixture
def aqi_last_hour_fixture():
    columns = [
        "DATETIME",
        "TANGARA_2BBA",
        "TANGARA_14D6",
        "TANGARA_1CE2",
        "TANGARA_1FCA",
        "TANGARA_2492",
        "TANGARA_2FF6",
        "TANGARA_48C6",
    ]
    values = np.array(
        get_random_data_sensors()
    )

    return pd.DataFrame(values, columns=columns).astype(
        dtype={
            "DATETIME": "object",
            "TANGARA_2BBA": "Int64",
            "TANGARA_14D6": "Int64",
            "TANGARA_1CE2": "Int64",
            "TANGARA_1FCA": "Int64",
            "TANGARA_2492": "Int64",
            "TANGARA_2FF6": "Int64",
            "TANGARA_48C6": "Int64",
        }
    )

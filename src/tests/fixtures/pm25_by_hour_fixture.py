"""
This fixture contains the definition of pm25_by_hour DataFrame used by Nodes and Pipelines.
"""
import pytest
import pandas as pd
import numpy as np


@pytest.fixture
def pm25_by_hour_fixture():
    columns = [
        "DATATIME",
        "TANGARA_1FCA",
        "TANGARA_48C6",
        "DATE",
        "TIME",
        "WEEKDAY",
        "MONTH",
        "YEAR"
    ]
    values = np.array(
        [
            ["2022-09-04T01:00:00-05:00", 5, 4, "09/04/22", "01:00:00", "Sunday", "September", 2022],
            ["2022-09-04T02:00:00-05:00", 5, 2, "09/04/22", "02:00:00", "Sunday", "September", 2022],
            ["2022-09-04T03:00:00-05:00", 4, None, "09/04/22", "03:00:00", "Sunday", "September", 2022],
            ["2022-09-04T04:00:00-05:00", 5, 4, "09/04/22", "04:00:00", "Sunday", "September", 2022],
            ["2022-09-04T05:00:00-05:00", 2, 4, "09/04/22", "05:00:00", "Sunday", "September", 2022],
            ["2022-09-04T06:00:00-05:00", 3, 1, "09/04/22", "06:00:00", "Sunday", "September", 2022],
            ["2022-09-04T07:00:00-05:00", 5, None, "09/04/22", "07:00:00", "Sunday", "September", 2022],
            ["2022-09-04T08:00:00-05:00", None, 4, "09/04/22", "08:00:00", "Sunday", "September", 2022],
            ["2022-09-04T09:00:00-05:00", 1, 3, "09/04/22", "00:09:00", "Sunday", "September", 2022],
            ["2022-09-04T10:00:00-05:00", None, None, "09/04/22", "10:00:00", "Sunday", "September", 2022],
        ]
    )

    return pd.DataFrame(values, columns=columns).astype(
        dtype={
            "DATATIME": "object",
            "TANGARA_1FCA": "float64",
            "TANGARA_48C6": "float64",
        }
    )

"""
This fixture contains the definition of pm25 DataFrame used by Nodes and Pipelines.
"""
import pytest
import pandas as pd
import numpy as np


@pytest.fixture
def pm25_fixture():
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
            ["2022-09-04T00:00:00-05:00", 5, 4, "09/04/22", "00:00:00", "Sunday", "September", 2022],
            ["2022-09-04T00:00:30-05:00", 5, 2, "09/04/22", "00:00:30", "Sunday", "September", 2022],
            ["2022-09-04T00:01:00-05:00", 4, None, "09/04/22", "00:01:00", "Sunday", "September", 2022],
            ["2022-09-04T00:01:30-05:00", 5, 4, "09/04/22", "00:01:30", "Sunday", "September", 2022],
            ["2022-09-04T00:02:00-05:00", 2, 4, "09/04/22", "00:02:00", "Sunday", "September", 2022],
            ["2022-09-04T00:02:30-05:00", 3, 1, "09/04/22", "00:02:30", "Sunday", "September", 2022],
            ["2022-09-04T00:03:00-05:00", 5, None, "09/04/22", "00:03:00", "Sunday", "September", 2022],
            ["2022-09-04T00:03:30-05:00", None, 4, "09/04/22", "00:03:30", "Sunday", "September", 2022],
            ["2022-09-04T00:04:00-05:00", 1, 3, "09/04/22", "00:04:00", "Sunday", "September", 2022],
            ["2022-09-04T00:04:30-05:00", None, None, "09/04/22", "00:04:30", "Sunday", "September", 2022],
        ]
    )

    return pd.DataFrame(values, columns=columns).astype(
        dtype={
            "DATATIME": "object",
            "TANGARA_1FCA": "float64",
            "TANGARA_48C6": "float64",
        }
    )

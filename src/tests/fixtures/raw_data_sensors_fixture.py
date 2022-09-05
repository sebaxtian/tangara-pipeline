"""
This fixture contains the definition of raw_data_sensors DataFrame used by Nodes and Pipelines.
"""
import pytest
import pandas as pd
import numpy as np


@pytest.fixture
def raw_data_sensors_fixture():
    columns = [
        "DATATIME",
        "TANGARA_1FCA",
        "TANGARA_48C6",
    ]
    values = np.array(
        [
            ["2022-03-30T00:00:00-05:00", 5, 4],
            ["2022-03-30T00:00:30-05:00", 5, 2],
            ["2022-03-30T00:01:00-05:00", 4, None],
            ["2022-03-30T00:01:30-05:00", 5, 4],
            ["2022-03-30T00:02:00-05:00", 2, 4],
            ["2022-03-30T00:02:30-05:00", 3, 1],
            ["2022-03-30T00:03:00-05:00", 5, None],
            ["2022-03-30T00:03:30-05:00", None, 4],
            ["2022-03-30T00:04:00-05:00", 1, 3],
            ["2022-03-30T00:04:30-05:00", None, None],
        ]
    )

    return pd.DataFrame(values, columns=columns).astype(
        dtype={
            "DATATIME": "object",
            "TANGARA_1FCA": "float64",
            "TANGARA_48C6": "float64",
        }
    )

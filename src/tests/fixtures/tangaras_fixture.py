"""
This fixture contains the definition of tangaras DataFrame used by Nodes and Pipelines.
"""
import pytest
import pandas as pd
import numpy as np


@pytest.fixture
def tangaras_fixture():
    columns = [
        "ID",
        "GEOHASH",
        "MAC",
        "GEOLOCATION",
        "LATITUDE",
        "LONGITUDE",
        "DATETIME",
    ]
    values = np.array(
        [
            [
                "TANGARA_48C6",
                "d29ee19",
                "D29TTGOT7D48C6",
                "3.4366607666015625 -76.50672912597656",
                3.4366607666015625,
                -76.50672912597656,
                "2022-09-05 12:12:33.762188-05:00",
            ],
            [
                "TANGARA_FAC6",
                "d29ed5r",
                "D29TTGOTD8FAC6",
                "3.4462738037109375 -76.54243469238281",
                3.4462738037109375,
                -76.54243469238281,
                "2022-09-05 12:12:33.762188-05:00",
            ],
            [
                "TANGARA_F1AE",
                "d29eg66",
                "D29TTGOTD8F1AE",
                "3.4847259521484375 -76.49436950683594",
                3.4847259521484375,
                -76.49436950683594,
                "2022-09-05 12:12:33.762188-05:00",
            ],
        ]
    )
    return pd.DataFrame(values, columns=columns)

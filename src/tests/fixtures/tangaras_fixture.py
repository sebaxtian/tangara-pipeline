"""
This fixture contains the definition of tangaras DataFrame used by Nodes and Pipelines.
"""
import pytest
import pandas as pd
import numpy as np


@pytest.fixture
def tangaras_fixture():
    columns = ["MAC", "Label_ID", "Geolocation", "Status"]
    values = np.array(
        [
            ["D29ESP32DED1FCA", "Tangara_1FCA", "", "Offline"],
            ["D29TTGOT7D48C6", "CanAirIO_48C6", "3.446018 -76.541824", "Online"],
        ]
    )
    return pd.DataFrame(values, columns=columns)

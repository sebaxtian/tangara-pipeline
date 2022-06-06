"""
This fixture contains the definition of spreadsheets DataFrame used by Nodes and Pipelines.
"""
import pytest
import pandas as pd
import numpy as np


@pytest.fixture
def spreadsheets_fixture():
    columns = ["ID", "Name", "URL"]
    values = np.array(
        [
            [
                1,
                "Week 1",
                "https://docs.google.com/spreadsheets/d/1pSX8FgewRDUq6qGrw_HpBYRh1sYUv8A8rcvAAYTfLrk/edit?usp=sharing",
            ],
            [
                5,
                "Week 5",
                "https://docs.google.com/spreadsheets/d/15-CwLfxx4LeB54627SjzS_4s7cjFFAmx2vHTHKvjw9k/edit?usp=sharing",
            ],
        ]
    )
    return pd.DataFrame(values, columns=columns)

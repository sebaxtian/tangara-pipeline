"""
This is a boilerplate test file for pipeline 'pm25'
generated using Kedro 0.18.1.
Please add your pipeline tests here.

Kedro recommends using `pytest` framework, more info about it can be found
in the official documentation:
https://docs.pytest.org/en/latest/getting-started.html
"""
import pytest
from datetime import datetime
import pandas as pd


from tests.fixtures.raw_data_sensors_fixture import raw_data_sensors_fixture
from tangara_pipeline.pipelines.pm25 import create_pipeline
from tangara_pipeline.pipelines.pm25.nodes import _add_datetime_str_values
from tangara_pipeline.pipelines.pm25.nodes import _get_date_str
from tangara_pipeline.pipelines.pm25.nodes import _get_time_str
from tangara_pipeline.pipelines.pm25.nodes import _get_weekday_str
from tangara_pipeline.pipelines.pm25.nodes import _get_month_str
from tangara_pipeline.pipelines.pm25.nodes import _get_year_str


@pytest.fixture
def pm25_pipeline():
    return create_pipeline()


@pytest.fixture
def timestamp_fixture():
    ts_value = datetime.strptime("2022-03-30 00:01:30", "%Y-%m-%d %H:%M:%S")
    timestamp_dic = {
        "Timestamp": ts_value,
        "Date": ts_value.strftime("%x"),
        "Time": ts_value.strftime("%T"),
        "Weekday": ts_value.strftime("%A"),
        "Month": ts_value.strftime("%B"),
        "Year": ts_value.strftime("%Y"),
    }
    return timestamp_dic


class TestPM25:
    def test_pipeline(self, pm25_pipeline):
        inputs = pm25_pipeline.inputs()
        outputs = pm25_pipeline.outputs()

        assert inputs.issubset(set(["raw_data_sensors"]))
        assert outputs.issubset(set(["pm25"]))

    def test_nodes(self, pm25_pipeline, raw_data_sensors_fixture):
        pm25_nodes = pm25_pipeline.nodes

        assert len(pm25_nodes) == 1
        assert pm25_nodes[0].name == "pm25_node"
        assert pm25_nodes[0]._func_name == "pm25"

        pm25 = pm25_nodes[0].func(raw_data_sensors_fixture)
        assert pm25.empty == False
        assert pm25.columns.to_list() == [
            "Datetime",
            "Tangara_1FCA",
            "CanAirIO_48C6",
            "Timestamp",
            "Date",
            "Time",
            "Weekday",
            "Month",
            "Year",
        ]

    def test__add_datetime_str_values(self, raw_data_sensors_fixture):
        # Timestamp
        raw_data_sensors_fixture["Timestamp"] = pd.to_datetime(raw_data_sensors_fixture["Datetime"])

        assert _add_datetime_str_values(raw_data_sensors_fixture).columns.to_list() == [
            "Datetime",
            "Tangara_1FCA",
            "CanAirIO_48C6",
            "Timestamp",
            "Date",
            "Time",
            "Weekday",
            "Month",
            "Year",
        ]

    def test__datetime_str_values(self, timestamp_fixture):
        assert (
            _get_date_str(timestamp_fixture["Timestamp"]) == timestamp_fixture["Date"]
        )
        assert (
            _get_time_str(timestamp_fixture["Timestamp"]) == timestamp_fixture["Time"]
        )
        assert (
            _get_weekday_str(timestamp_fixture["Timestamp"])
            == timestamp_fixture["Weekday"]
        )
        assert (
            _get_month_str(timestamp_fixture["Timestamp"]) == timestamp_fixture["Month"]
        )
        assert (
            _get_year_str(timestamp_fixture["Timestamp"]) == timestamp_fixture["Year"]
        )

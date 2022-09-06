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
from sqlalchemy import column


from tests.fixtures.raw_data_sensors_api_fixture import raw_data_sensors_api_fixture
from tangara_pipeline.pipelines.pm25 import create_pipeline
from tangara_pipeline.pipelines.pm25.nodes import _add_datetime_str_values


@pytest.fixture
def pm25_pipeline():
    return create_pipeline()


@pytest.fixture
def timestamp_fixture():
    ts_value = datetime.strptime("2022-09-04 00:00:00", "%Y-%m-%d %H:%M:%S")
    timestamp_dic = {
        "DATETIME": ts_value.strftime("%Y-%m-%d %H:%M:%S"),
        "DATE": ts_value.strftime("%x"),
        "TIME": ts_value.strftime("%T"),
        "WEEKDAY": ts_value.strftime("%A"),
        "MONTH": ts_value.strftime("%B"),
        "YEAR": ts_value.strftime("%Y"),
    }
    return timestamp_dic


class TestPM25:
    def test_pipeline(self, pm25_pipeline):
        inputs = pm25_pipeline.inputs()
        outputs = pm25_pipeline.outputs()

        assert inputs.issubset(set(["raw_data_sensors_api"]))
        assert outputs.issubset(set(["pm25", "pm25_by_hour", "pm25_movil_24h"]))

    def test_nodes(self, pm25_pipeline, raw_data_sensors_api_fixture):
        pm25_nodes = pm25_pipeline.nodes

        assert len(pm25_nodes) == 3
        assert pm25_nodes[0].name == "pm25_node"
        assert pm25_nodes[0]._func_name == "pm25"

        pm25 = pm25_nodes[0].func(raw_data_sensors_api_fixture)
        assert pm25.empty == False
        assert pm25.columns.to_list() == [
            "DATETIME",
            "TANGARA_1FCA",
            "TANGARA_48C6",
            "DATE",
            "TIME",
            "WEEKDAY",
            "MONTH",
            "YEAR",
        ]

        assert pm25_nodes[1].name == "pm25_by_hour_node"
        assert pm25_nodes[1]._func_name == "pm25_by_hour"

        pm25_by_hour = pm25_nodes[1].func(raw_data_sensors_api_fixture)
        assert pm25_by_hour.empty == False
        assert pm25_by_hour.columns.to_list() == [
            "DATETIME",
            "TANGARA_1FCA",
            "TANGARA_48C6",
            "DATE",
            "TIME",
            "WEEKDAY",
            "MONTH",
            "YEAR",
        ]

        assert pm25_nodes[2].name == "pm25_movil_24h_node"
        assert pm25_nodes[2]._func_name == "pm25_movil_24h"

        pm25_movil_24h = pm25_nodes[2].func(raw_data_sensors_api_fixture)
        assert pm25_movil_24h.empty == False
        assert pm25_movil_24h.columns.to_list() == [
            "DATETIME",
            "TANGARA_1FCA",
            "TANGARA_48C6",
            "DATE",
            "TIME",
            "WEEKDAY",
            "MONTH",
            "YEAR",
        ]

    def test__add_datetime_str_values(self, raw_data_sensors_api_fixture, timestamp_fixture):
        # DateTime
        raw_data_sensors_api_fixture["DATETIME"] = pd.to_datetime(
            raw_data_sensors_api_fixture["DATETIME"]
        )

        raw_data_sensors_api_datetime = _add_datetime_str_values(raw_data_sensors_api_fixture)
        assert raw_data_sensors_api_datetime.columns.to_list() == [
            "DATETIME",
            "TANGARA_1FCA",
            "TANGARA_48C6",
            "DATE",
            "TIME",
            "WEEKDAY",
            "MONTH",
            "YEAR",
        ]
        assert raw_data_sensors_api_datetime['DATETIME'].iloc[0].strftime("%Y-%m-%d %H:%M:%S") == timestamp_fixture['DATETIME']
        assert raw_data_sensors_api_datetime['DATE'].iloc[0] == timestamp_fixture['DATE']
        assert raw_data_sensors_api_datetime['TIME'].iloc[0] == timestamp_fixture['TIME']
        assert raw_data_sensors_api_datetime['WEEKDAY'].iloc[0] == timestamp_fixture['WEEKDAY']
        assert raw_data_sensors_api_datetime['MONTH'].iloc[0] == timestamp_fixture['MONTH']
        assert raw_data_sensors_api_datetime['YEAR'].iloc[0] == timestamp_fixture['YEAR']

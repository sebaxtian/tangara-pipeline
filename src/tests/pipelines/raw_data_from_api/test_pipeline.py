"""
This is a boilerplate test file for pipeline 'raw_data_from_api'
generated using Kedro 0.18.1.
Please add your pipeline tests here.

Kedro recommends using `pytest` framework, more info about it can be found
in the official documentation:
https://docs.pytest.org/en/latest/getting-started.html
"""
import pytest
import re

from tests.fixtures.tangaras_fixture import tangaras_fixture
from tangara_pipeline.pipelines.raw_data_from_api import create_pipeline
from tangara_pipeline.pipelines.raw_data_from_api.nodes import _get_period_time
from tangara_pipeline.pipelines.raw_data_from_api.nodes import _get_sql_query_tangaras
from tangara_pipeline.pipelines.raw_data_from_api.nodes import _get_sql_query_sensors
from tangara_pipeline.pipelines.raw_data_from_api.nodes import _request_to_influxdb


@pytest.fixture
def raw_data_sensors_api_pipeline():
    return create_pipeline()


class TestRawDataFromAPI:
    def test_pipeline(self, raw_data_sensors_api_pipeline):
        inputs = raw_data_sensors_api_pipeline.inputs()
        outputs = raw_data_sensors_api_pipeline.outputs()

        assert inputs.issubset(
            set(["tangaras", "params:start_datetime", "params:end_datetime"])
        )
        assert outputs.issubset(set(["tangaras", "raw_data_sensors_api"]))

    def test_nodes(self, raw_data_sensors_api_pipeline, tangaras_fixture):
        raw_data_sensors_api_nodes = raw_data_sensors_api_pipeline.nodes

        assert len(raw_data_sensors_api_nodes) == 2
        assert raw_data_sensors_api_nodes[0].name == "tangaras_node"
        assert raw_data_sensors_api_nodes[0]._func_name == "tangaras"
        assert raw_data_sensors_api_nodes[1].name == "raw_data_sensors_api_node"
        assert raw_data_sensors_api_nodes[1]._func_name == "raw_data_sensors"

        start_datetime = "2022-09-05T13:20:00"
        end_datetime = "2022-09-05T13:22:00"

        tangaras = raw_data_sensors_api_nodes[0].func(start_datetime, end_datetime)
        assert tangaras.empty == False
        assert tangaras.columns.to_list() == [
            "ID",
            "GEOHASH",
            "MAC",
            "GEOLOCATION",
            "LATITUDE",
            "LONGITUDE",
            "DATETIME",
        ]

        raw_data_sensors = raw_data_sensors_api_nodes[1].func(
            tangaras_fixture, start_datetime, end_datetime
        )
        assert raw_data_sensors.empty == False
        assert raw_data_sensors.columns.to_list() == [
            "DATETIME",
            "TANGARA_48C6",
            "TANGARA_FAC6",
            "TANGARA_F1AE",
        ]

    def test__get_period_time(self):
        period_time = _get_period_time("2022-09-05T13:20:00", "2022-09-05T13:22:00")
        assert re.search(r"time >= [0-9]*ms and time <= [0-9]*ms", period_time)

    def test__get_sql_query_tangaras(self):
        sql_query = _get_sql_query_tangaras("2022-09-05T13:20:00", "2022-09-05T13:22:00")

        pattern = r'SELECT DISTINCT\(geo\) AS "geohash" FROM "fixed_stations_01" WHERE \("geo3" = \'d29\'\) AND time >= [0-9]*ms and time <= [0-9]*ms GROUP BY "name";'
        assert re.search(pattern, sql_query)

    def test__get_sql_query_sensors(self, tangaras_fixture):
        sql_query = _get_sql_query_sensors(
            tangaras_fixture, "2022-09-05T13:20:00", "2022-09-05T13:22:00"
        )

        pattern = r'SELECT "name", last\("pm25"\) FROM "fixed_stations_01" WHERE \("name" = \'([A-Z]*[0-9]*)*\'\) AND time >= [0-9]*ms and time <= [0-9]*ms GROUP BY time\(30s\) fill\(none\);? ?'
        assert re.search(pattern, sql_query)

    def test__request_to_influxdb(self, tangaras_fixture):
        sql_query = _get_sql_query_sensors(
            tangaras_fixture, "2022-09-05T13:20:00", "2022-09-05T13:22:00"
        )

        response_influxdb = _request_to_influxdb(sql_query)
        assert response_influxdb.status_code == 200

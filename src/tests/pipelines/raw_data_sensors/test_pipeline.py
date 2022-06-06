"""
This is a boilerplate test file for pipeline 'raw_data_sensors'
generated using Kedro 0.18.1.
Please add your pipeline tests here.

Kedro recommends using `pytest` framework, more info about it can be found
in the official documentation:
https://docs.pytest.org/en/latest/getting-started.html
"""
import pytest

from tests.fixtures.tangaras_fixture import tangaras_fixture
from tests.fixtures.spreadsheets_fixture import spreadsheets_fixture
from tangara_pipeline.pipelines.raw_data_sensors import create_pipeline
from tangara_pipeline.pipelines.raw_data_sensors.nodes import raw_data_sensors


@pytest.fixture
def raw_data_sensors_pipeline():
    return create_pipeline()


class TestRawDataSensors:
    def test_pipeline(self, raw_data_sensors_pipeline):
        inputs = raw_data_sensors_pipeline.inputs()
        outputs = raw_data_sensors_pipeline.outputs()

        assert inputs.issubset(
            set(
                [
                    "tangaras",
                    "spreadsheets",
                    "params:start_datetime",
                    "params:end_datetime",
                    "params:raw_data_origin",
                ]
            )
        )
        assert outputs.issubset(set(["raw_data_sensors"]))

    def test_nodes(
        self, raw_data_sensors_pipeline, tangaras_fixture, spreadsheets_fixture
    ):
        raw_data_sensors_nodes = raw_data_sensors_pipeline.nodes

        assert len(raw_data_sensors_nodes) == 1
        assert raw_data_sensors_nodes[0].name == "raw_data_sensors_node"
        assert raw_data_sensors_nodes[0]._func_name == "raw_data_sensors"

        start_datetime = "2022-04-05T09:00:00"
        end_datetime = "2022-04-05T09:01:00"
        raw_data_origin = ["csv", "api"]

        for value in raw_data_origin:
            raw_data_sensors = raw_data_sensors_nodes[0].func(
                tangaras_fixture,
                spreadsheets_fixture,
                start_datetime,
                end_datetime,
                value,
            )
            assert raw_data_sensors.empty == False
            assert raw_data_sensors.columns.to_list() == [
                "Datetime",
                "Tangara_1FCA",
                "CanAirIO_48C6",
            ]

    def test_raw_data_sensors(self, tangaras_fixture, spreadsheets_fixture):
        start_datetime = "2022-04-05T09:00:00"
        end_datetime = "2022-04-05T09:01:00"
        raw_data_origin = ["csv", "api", "other"]

        for value in raw_data_origin:
            raw_data = raw_data_sensors(
                tangaras_fixture,
                spreadsheets_fixture,
                start_datetime,
                end_datetime,
                value,
            )
            if value == "other":
                assert raw_data.empty == True
            else:
                assert raw_data.empty == False
                assert raw_data.columns.to_list() == [
                    "Datetime",
                    "Tangara_1FCA",
                    "CanAirIO_48C6",
                ]

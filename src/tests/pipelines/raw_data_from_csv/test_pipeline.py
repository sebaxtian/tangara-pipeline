"""
This is a boilerplate test file for pipeline 'raw_data_from_csv'
generated using Kedro 0.18.1.
Please add your pipeline tests here.

Kedro recommends using `pytest` framework, more info about it can be found
in the official documentation:
https://docs.pytest.org/en/latest/getting-started.html

To run the tests, run ``kedro test`` from the project root directory.
"""
import pytest

from tests.fixtures.tangaras_fixture import tangaras_fixture
from tests.fixtures.spreadsheets_fixture import spreadsheets_fixture
from tangara_pipeline.pipelines.raw_data_from_csv import create_pipeline
from tangara_pipeline.pipelines.raw_data_from_csv.nodes import _convert_gsheets_url


@pytest.fixture
def raw_data_sensors_csv_pipeline():
    return create_pipeline()


class TestRawDataFromCSV:
    def test_pipeline(self, raw_data_sensors_csv_pipeline):
        inputs = raw_data_sensors_csv_pipeline.inputs()
        outputs = raw_data_sensors_csv_pipeline.outputs()

        assert inputs.issubset(set(["tangaras", "spreadsheets"]))
        assert outputs.issubset(set(["raw_data_sensors_csv"]))

    def test_nodes(
        self, raw_data_sensors_csv_pipeline, tangaras_fixture, spreadsheets_fixture
    ):
        raw_data_sensors_csv_nodes = raw_data_sensors_csv_pipeline.nodes

        assert len(raw_data_sensors_csv_nodes) == 1
        assert raw_data_sensors_csv_nodes[0].name == "raw_data_sensors_csv_node"
        assert raw_data_sensors_csv_nodes[0]._func_name == "raw_data_sensors"

        raw_data_sensors = raw_data_sensors_csv_nodes[0].func(
            tangaras_fixture, spreadsheets_fixture
        )
        assert raw_data_sensors.empty == False
        assert raw_data_sensors.columns.to_list() == [
            "Datetime",
            "Tangara_1FCA",
            "CanAirIO_48C6",
        ]

    def test__convert_gsheets_url(self, spreadsheets_fixture):
        gsheets_url = spreadsheets_fixture.iloc[[0]]["URL"].to_numpy()[0]
        url_csv = _convert_gsheets_url(gsheets_url)

        assert url_csv.split("/")[-1] == "export?format=csv"

"""
This is a boilerplate test file for pipeline 'pm25_influxdb_aqi'
generated using Kedro 0.18.1.
Please add your pipeline tests here.

Kedro recommends using `pytest` framework, more info about it can be found
in the official documentation:
https://docs.pytest.org/en/latest/getting-started.html
"""
import pytest

from tangara_pipeline.pipelines.pm25_influxdb_aqi import create_pipeline


@pytest.fixture
def pm25_influxdb_aqi_pipeline():
    return create_pipeline()


class TestPM25InfluxDBAQI:
    def test_pipeline(self, pm25_influxdb_aqi_pipeline):
        inputs = pm25_influxdb_aqi_pipeline.inputs()
        outputs = pm25_influxdb_aqi_pipeline.outputs()

        assert inputs.issubset(
            set(
                [
                    "params:influxdb_version",
                    "params:pm25_raw_measurement_name",
                    "params:pm25_clean_measurement_name",
                    "params:pm25_last_hour_measurement_name",
                    "params:pm25_last_8h_measurement_name",
                    "params:pm25_last_12h_measurement_name",
                    "params:pm25_last_24h_measurement_name",
                    "params:aqi_instant_measurement_name",
                    "params:aqi_last_hour_measurement_name",
                    "params:aqi_last_8h_measurement_name",
                    "params:aqi_last_12h_measurement_name",
                    "params:aqi_last_24h_measurement_name",
                    "pm25_raw",
                    "pm25_clean",
                    "pm25_last_hour",
                    "pm25_last_8h",
                    "pm25_last_12h",
                    "pm25_last_24h",
                    "aqi_instant",
                    "aqi_last_hour",
                    "aqi_last_8h",
                    "aqi_last_12h",
                    "aqi_last_24h",
                ]
            )
        )
        assert not outputs == False

    def test_nodes(self, pm25_influxdb_aqi_pipeline):
        pm25_influxdb_aqi_nodes = pm25_influxdb_aqi_pipeline.nodes
        assert len(pm25_influxdb_aqi_nodes) == 11

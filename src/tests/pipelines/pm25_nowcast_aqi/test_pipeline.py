"""
This is a boilerplate test file for pipeline 'pm25_nowcast_aqi'
generated using Kedro 0.18.1.
Please add your pipeline tests here.

Kedro recommends using `pytest` framework, more info about it can be found
in the official documentation:
https://docs.pytest.org/en/latest/getting-started.html
"""
import pytest

from tests.fixtures.pm25_raw_fixture import pm25_raw_fixture
from tangara_pipeline.pipelines.pm25_nowcast_aqi import create_pipeline


@pytest.fixture
def pm25_nowcast_aqi_pipeline():
    return create_pipeline()


class TestPM25NowCastAQI:
    def test_pipeline(self, pm25_nowcast_aqi_pipeline):
        inputs = pm25_nowcast_aqi_pipeline.inputs()
        outputs = pm25_nowcast_aqi_pipeline.outputs()

        assert inputs.issubset(set(["params:nowcast_datetime", "pm25_raw", "pm25_clean", "pm25_last_hour"]))
        assert outputs.issubset(set(["tangaras", "pm25_raw", "pm25_clean", "pm25_last_hour", "pm25_last_8h", "pm25_last_12h", "pm25_last_24h"]))

    def test_nodes(self, pm25_nowcast_aqi_pipeline, pm25_raw_fixture):
        pm25_nodes = pm25_nowcast_aqi_pipeline.nodes
        print(pm25_nodes)
        assert len(pm25_nodes) == 6

        nowcast_datetime = '2022-09-06T13:35:00'

        for node in pm25_nodes:
            if node._func_name == "pm25_raw":
                # Node pm25_raw
                tangaras, pm25_raw = node.func(nowcast_datetime)
                assert tangaras.empty == False
                assert pm25_raw.empty == False
                assert pm25_raw[pm25_raw_fixture.columns].columns.to_list() == pm25_raw_fixture.columns.to_list()
            if node._func_name == "pm25_clean":
                # Node pm25_clean
                pm25_clean = node.func(pm25_raw_fixture)
                assert pm25_clean.empty == False
                assert pm25_clean.columns.to_list() == pm25_raw_fixture.columns.to_list()
            if node._func_name == "pm25_last_hour":
                # Node pm25_last_hour
                pm25_last_hour = node.func(pm25_clean)
                assert pm25_last_hour.empty == False
                assert pm25_last_hour.columns.to_list() == pm25_raw_fixture.columns.to_list()
            if node._func_name == "pm25_last_8h":
                # Node pm25_last_8h
                pm25_last_8h = node.func(pm25_last_hour)
                assert pm25_last_8h.empty == False
                assert pm25_last_8h.columns.to_list() == pm25_raw_fixture.columns.to_list()
            if node._func_name == "pm25_last_12h":
                # Node pm25_last_12h
                pm25_last_12h = node.func(pm25_last_hour)
                assert pm25_last_12h.empty == False
                assert pm25_last_12h.columns.to_list() == pm25_raw_fixture.columns.to_list()
            if node._func_name == "pm25_last_24h":
                # Node pm25_last_24h
                pm25_last_24h = node.func(pm25_last_hour)
                assert pm25_last_24h.empty == False
                assert pm25_last_24h.columns.to_list() == pm25_raw_fixture.columns.to_list()

"""
This is a boilerplate test file for pipeline 'pm25_nowcast_aqi'
generated using Kedro 0.18.1.
Please add your pipeline tests here.

Kedro recommends using `pytest` framework, more info about it can be found
in the official documentation:
https://docs.pytest.org/en/latest/getting-started.html
"""
import pytest

from tangara_pipeline.pipelines.pm25_nowcast_aqi import create_pipeline
from tests.fixtures.aqi_instant_fixture import aqi_instant_fixture
from tests.fixtures.aqi_last_hour_fixture import aqi_last_hour_fixture
from tests.fixtures.pm25_clean_fixture import pm25_clean_fixture


@pytest.fixture
def pm25_nowcast_aqi_pipeline():
    return create_pipeline()

class TestPM25NowCastAQI:
    def test_pipeline(self, pm25_nowcast_aqi_pipeline):
        inputs = pm25_nowcast_aqi_pipeline.inputs()
        outputs = pm25_nowcast_aqi_pipeline.outputs()

        assert inputs.issubset(set(["pm25_clean", "pm25_last_hour", "pm25_last_8h", "pm25_last_12h", "pm25_last_24h"]))
        assert outputs.issubset(set(["aqi_instant", "aqi_last_hour", "aqi_last_8h", "aqi_last_12h", "aqi_last_24h"]))
    
    def test_nodes(self, pm25_nowcast_aqi_pipeline, pm25_clean_fixture, aqi_instant_fixture, aqi_last_hour_fixture):
        pm25_nowcast_aqi_nodes = pm25_nowcast_aqi_pipeline.nodes
        print(pm25_nowcast_aqi_nodes)
        assert len(pm25_nowcast_aqi_nodes) == 5

        for node in pm25_nowcast_aqi_nodes:
            if node._func_name == "aqi_instant":
                # Node aqi_instant
                aqi_instant = node.func(pm25_clean_fixture)
                assert aqi_instant.empty == False
                assert aqi_instant.columns.to_list() == pm25_clean_fixture.columns.to_list()
            if node._func_name == "aqi_last_hour":
                # Node aqi_last_hour
                aqi_last_hour = node.func(aqi_instant_fixture)
                assert aqi_last_hour.empty == False
                assert aqi_last_hour.columns.to_list() == aqi_instant_fixture.columns.to_list()
            if node._func_name == "aqi_last_8h":
                # Node aqi_last_8h
                aqi_last_8h = node.func(aqi_last_hour_fixture)
                assert aqi_last_8h.empty == False
                assert aqi_last_8h.columns.to_list() == aqi_last_hour_fixture.columns.to_list()
            if node._func_name == "aqi_last_12h":
                # Node aqi_last_12h
                aqi_last_12h = node.func(aqi_last_hour_fixture)
                assert aqi_last_12h.empty == False
                assert aqi_last_12h.columns.to_list() == aqi_last_hour_fixture.columns.to_list()
            if node._func_name == "aqi_last_24h":
                # Node aqi_last_24h
                aqi_last_24h = node.func(aqi_last_hour_fixture)
                assert aqi_last_24h.empty == False
                assert aqi_last_24h.columns.to_list() == aqi_last_hour_fixture.columns.to_list()

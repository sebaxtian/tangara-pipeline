"""
This is a boilerplate test file for pipeline 'tangara_stations'
generated using Kedro 0.18.1.
Please add your pipeline tests here.

Kedro recommends using `pytest` framework, more info about it can be found
in the official documentation:
https://docs.pytest.org/en/latest/getting-started.html
"""
import pytest
from datetime import datetime

from tangara_pipeline.pipelines.tangara_stations import create_pipeline
from tangara_pipeline.pipelines.tangara_stations.nodes import get_start_nowcast_timestamp

@pytest.fixture
def tangara_stations_pipeline():
    return create_pipeline()


class TestTangaraStations:
    def test_pipeline(self, tangara_stations_pipeline):
        inputs = tangara_stations_pipeline.all_inputs()
        outputs = tangara_stations_pipeline.all_outputs()

        assert len(inputs) == 3
        assert len(outputs) == 5
        assert inputs.issubset(set(["tangara_stations", "params:nowcast_datetime", "params:start_datetime"]))
        assert outputs.issubset(set(["tangara_stations", "pm25_raw", "temp_raw", "hum_raw", "co2_raw"]))
    
    def test_nodes(self, tangara_stations_pipeline):
        tangara_stations_nodes = tangara_stations_pipeline.nodes
        assert len(tangara_stations_nodes) == 5

        nowcast_datetime = '2022-09-06T13:35:00'

        for node in tangara_stations_nodes:
            if node._func_name == "tangara_stations":
                # Node tangara_stations
                tangara_stations = node.func(nowcast_datetime)
                assert tangara_stations.empty == False
                assert tangara_stations.columns.to_list() == ['DATETIME', 'ID', 'MAC', 'GEOHASH', 'GEOLOCATION', 'LATITUDE', 'LONGITUDE']
            if node._func_name == "pm25_raw":
                # Node pm25_raw
                pm25_raw = node.func(tangara_stations, nowcast_datetime)
                assert pm25_raw.empty == False
                assert 'DATETIME' in pm25_raw.columns.to_list()
            if node._func_name == "temp_raw":
                # Node temp_raw
                temp_raw = node.func(tangara_stations, nowcast_datetime)
                assert temp_raw.empty == False
                assert 'DATETIME' in temp_raw.columns.to_list()
            if node._func_name == "hum_raw":
                # Node hum_raw
                hum_raw = node.func(tangara_stations, nowcast_datetime)
                assert hum_raw.empty == False
                assert 'DATETIME' in hum_raw.columns.to_list()
            if node._func_name == "co2_raw":
                # Node co2_raw
                co2_raw = node.func(tangara_stations, nowcast_datetime)
                assert co2_raw.empty == False
                assert 'DATETIME' in co2_raw.columns.to_list()

    def test_get_start_nowcast_timestamp(self):
        nowcast_datetime = '2022-09-06T13:35:00'
        start_timestamp, nowcast_timestamp = get_start_nowcast_timestamp(nowcast_datetime)

        tdelta = int((datetime.fromtimestamp(nowcast_timestamp / 1000) - datetime.fromtimestamp(start_timestamp / 1000)).total_seconds()/(60*60))
        assert tdelta > 0
        assert tdelta == 24

        nowcast_datetime = '2022-09-06T13:35:00'
        start_datetime = '2022-09-05T21:40:10'
        start_timestamp, nowcast_timestamp = get_start_nowcast_timestamp(nowcast_datetime, start_datetime)

        tdelta = int((datetime.fromtimestamp(nowcast_timestamp / 1000) - datetime.fromtimestamp(start_timestamp / 1000)).total_seconds()/(60*60))
        assert tdelta > 0
        assert tdelta == 15

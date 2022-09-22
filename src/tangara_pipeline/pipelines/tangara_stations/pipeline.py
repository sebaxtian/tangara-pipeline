"""
This is a boilerplate pipeline 'tangara_stations'
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import tangara_stations
from .nodes import pm25_raw
from .nodes import temp_raw
from .nodes import hum_raw
from .nodes import co2_raw


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=tangara_stations,
            inputs=['params:nowcast_datetime', 'params:start_datetime'],
            outputs='tangara_stations',
            name='tangara_stations_node'
        ),
        node(
            func=pm25_raw,
            inputs=['tangara_stations', 'params:nowcast_datetime', 'params:start_datetime'],
            outputs='pm25_raw',
            name='pm25_raw_node'
        ),
        node(
            func=temp_raw,
            inputs=['tangara_stations', 'params:nowcast_datetime', 'params:start_datetime'],
            outputs='temp_raw',
            name='temp_raw_node'
        ),
        node(
            func=hum_raw,
            inputs=['tangara_stations', 'params:nowcast_datetime', 'params:start_datetime'],
            outputs='hum_raw',
            name='hum_raw_node'
        ),
        node(
            func=co2_raw,
            inputs=['tangara_stations', 'params:nowcast_datetime', 'params:start_datetime'],
            outputs='co2_raw',
            name='co2_raw_node'
        )
    ])

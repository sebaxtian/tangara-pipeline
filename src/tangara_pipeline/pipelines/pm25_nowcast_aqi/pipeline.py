"""
This is a boilerplate pipeline 'pm25_nowcast_aqi'
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import aqi_instant
from .nodes import aqi_last_hour
from .nodes import aqi_last_8h
from .nodes import aqi_last_12h
from .nodes import aqi_last_24h


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=aqi_instant,
            inputs='pm25_clean',
            outputs='aqi_instant',
            name='aqi_instant_node'
        ),
        node(
            func=aqi_last_hour,
            inputs='pm25_last_hour',
            outputs='aqi_last_hour',
            name='aqi_last_hour_node'
        ),
        node(
            func=aqi_last_8h,
            inputs='pm25_last_8h',
            outputs='aqi_last_8h',
            name='aqi_last_8h_node'
        ),
        node(
            func=aqi_last_12h,
            inputs='pm25_last_12h',
            outputs='aqi_last_12h',
            name='aqi_last_12h_node'
        ),
        node(
            func=aqi_last_24h,
            inputs='pm25_last_24h',
            outputs='aqi_last_24h',
            name='aqi_last_24h_node'
        ),
    ])

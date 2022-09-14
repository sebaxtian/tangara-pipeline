"""
This is a boilerplate pipeline 'pm25_nowcast_aqi'
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import pm25_raw
from .nodes import pm25_clean
from .nodes import pm25_last_hour
from .nodes import pm25_last_8h
from .nodes import pm25_last_12h
from .nodes import pm25_last_24h


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=pm25_raw,
            inputs=['params:nowcast_datetime'],
            outputs=['tangaras', 'pm25_raw'],
            name='pm25_raw_node'
        ),
        node(
            func=pm25_clean,
            inputs='pm25_raw',
            outputs='pm25_clean',
            name='pm25_clean_node'
        ),
        node(
            func=pm25_last_hour,
            inputs='pm25_clean',
            outputs='pm25_last_hour',
            name='pm25_last_hour_node'
        ),
        node(
            func=pm25_last_8h,
            inputs='pm25_last_hour',
            outputs='pm25_last_8h',
            name='pm25_last_8h_node'
        ),
        node(
            func=pm25_last_12h,
            inputs='pm25_last_hour',
            outputs='pm25_last_12h',
            name='pm25_last_12h_node'
        ),
        node(
            func=pm25_last_24h,
            inputs='pm25_last_hour',
            outputs='pm25_last_24h',
            name='pm25_last_24h_node'
        )
    ])

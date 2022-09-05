"""
This is a boilerplate pipeline 'raw_data_from_api'
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import tangaras
from .nodes import raw_data_sensors


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=tangaras,
            inputs=['params:start_datetime' ,'params:end_datetime'],
            outputs='tangaras',
            name='tangaras_node'
        ),
        node(
            func=raw_data_sensors,
            inputs=['tangaras', 'params:start_datetime' ,'params:end_datetime'],
            outputs='raw_data_sensors_api',
            name='raw_data_sensors_api_node'
        )
    ])

"""
This is a boilerplate pipeline 'raw_data_sensors'
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import raw_data_sensors


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=raw_data_sensors,
            inputs=['tangaras', 'spreadsheets', 'params:start_datetime' ,'params:end_datetime', 'params:raw_data_origin'],
            outputs='raw_data_sensors',
            name='raw_data_sensors_node'
        )
    ])

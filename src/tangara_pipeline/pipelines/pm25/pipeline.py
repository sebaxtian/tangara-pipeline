"""
This is a boilerplate pipeline 'pm25'
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import pm25


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=pm25,
            inputs=['raw_data_sensors'],
            outputs='pm25',
            name='pm25_node'
        )
    ])

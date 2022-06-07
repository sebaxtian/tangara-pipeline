"""
This is a boilerplate pipeline 'pm25'
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import pm25
from .nodes import resample_pm25_by_hour


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=pm25,
            inputs='raw_data_sensors',
            outputs='pm25',
            name='pm25_node'
        ),
        node(
            func=resample_pm25_by_hour,
            inputs='pm25',
            outputs='pm25_by_hour',
            name='pm25_by_hour_node'
        )
    ])

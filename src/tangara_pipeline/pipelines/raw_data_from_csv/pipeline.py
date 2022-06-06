"""
This is a boilerplate pipeline 'raw_data_from_csv'
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import raw_data_sensors


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=raw_data_sensors,
            inputs=['tangaras', 'spreadsheets'],
            outputs='raw_data_sensors_csv',
            name='raw_data_sensors_csv_node'
        )
    ])

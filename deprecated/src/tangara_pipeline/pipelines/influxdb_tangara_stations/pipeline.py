"""
This is a boilerplate pipeline 'influxdb_tangara_stations'
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import ingesting_influxdb


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=ingesting_influxdb,
            inputs=['pm25_clean', 'temp_raw', 'hum_raw', 'co2_raw', 'aqi_instant', 'tangara_stations', 'params:influxdb_version'],
            outputs='stations_measurements',
            name='ingesting_influxdb_node'
        )
    ])

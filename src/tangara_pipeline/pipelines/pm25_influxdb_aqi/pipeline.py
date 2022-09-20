"""
This is a boilerplate pipeline 'pm25_influxdb_aqi'
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import ingesting_influxdb


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=ingesting_influxdb,
            inputs=['pm25_raw', 'params:pm25_raw_measurement_name', 'params:influxdb_version'],
            outputs=None,
            name='pm25_raw_node'
        ),
        node(
            func=ingesting_influxdb,
            inputs=['pm25_clean', 'params:pm25_clean_measurement_name', 'params:influxdb_version'],
            outputs=None,
            name='pm25_clean_node'
        ),
        node(
            func=ingesting_influxdb,
            inputs=['pm25_last_hour', 'params:pm25_last_hour_measurement_name', 'params:influxdb_version'],
            outputs=None,
            name='pm25_last_hour_node'
        ),
        node(
            func=ingesting_influxdb,
            inputs=['pm25_last_8h', 'params:pm25_last_8h_measurement_name', 'params:influxdb_version'],
            outputs=None,
            name='pm25_last_8h_node'
        ),
        node(
            func=ingesting_influxdb,
            inputs=['pm25_last_12h', 'params:pm25_last_12h_measurement_name', 'params:influxdb_version'],
            outputs=None,
            name='pm25_last_12h_node'
        ),
        node(
            func=ingesting_influxdb,
            inputs=['pm25_last_24h', 'params:pm25_last_24h_measurement_name', 'params:influxdb_version'],
            outputs=None,
            name='pm25_last_24h_node'
        ),
        node(
            func=ingesting_influxdb,
            inputs=['aqi_instant', 'params:aqi_instant_measurement_name', 'params:influxdb_version'],
            outputs=None,
            name='aqi_instant_node'
        ),
        node(
            func=ingesting_influxdb,
            inputs=['aqi_last_hour', 'params:aqi_last_hour_measurement_name', 'params:influxdb_version'],
            outputs=None,
            name='aqi_last_hour_node'
        ),
        node(
            func=ingesting_influxdb,
            inputs=['aqi_last_8h', 'params:aqi_last_8h_measurement_name', 'params:influxdb_version'],
            outputs=None,
            name='aqi_last_8h_node'
        ),
        node(
            func=ingesting_influxdb,
            inputs=['aqi_last_12h', 'params:aqi_last_12h_measurement_name', 'params:influxdb_version'],
            outputs=None,
            name='aqi_last_12h_node'
        ),
        node(
            func=ingesting_influxdb,
            inputs=['aqi_last_24h', 'params:aqi_last_24h_measurement_name', 'params:influxdb_version'],
            outputs=None,
            name='aqi_last_24h_node'
        ),
    ])

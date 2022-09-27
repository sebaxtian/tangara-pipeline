"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline, pipeline

from tangara_pipeline.pipelines import pm25
from tangara_pipeline.pipelines import pm25_nowcast_aqi
from tangara_pipeline.pipelines import pm25_influxdb_aqi
from tangara_pipeline.pipelines import tangara_stations
from tangara_pipeline.pipelines import influxdb_tangara_stations


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    pm25_pipeline = pm25.create_pipeline()
    pm25_nowcast_aqi_pipeline = pm25_nowcast_aqi.create_pipeline()
    pm25_influxdb_aqi_pipeline = pm25_influxdb_aqi.create_pipeline()
    tangara_stations_pipeline = tangara_stations.create_pipeline()
    influxdb_tangara_stations_pipeline = influxdb_tangara_stations.create_pipeline()

    return {
        "__default__": tangara_stations_pipeline,
        "tangara_stations": tangara_stations_pipeline,
        "pm25": pm25_pipeline,
        "pm25_nowcast_aqi": pm25_nowcast_aqi_pipeline,
        "pm25_influxdb_aqi": pm25_influxdb_aqi_pipeline,
        "influxdb_tangara_stations": influxdb_tangara_stations_pipeline,
    }

"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline, pipeline

from tangara_pipeline.pipelines import raw_data_from_csv
from tangara_pipeline.pipelines import raw_data_from_api
from tangara_pipeline.pipelines import raw_data_sensors
from tangara_pipeline.pipelines import pm25


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    raw_data_from_csv_pipeline = raw_data_from_csv.create_pipeline()
    raw_data_from_api_pipeline = raw_data_from_api.create_pipeline()
    raw_data_sensors_pipeline = raw_data_sensors.create_pipeline()
    pm25_pipeline = pm25.create_pipeline()

    return {
        "__default__": raw_data_sensors_pipeline,
        "raw_data_from_csv": raw_data_from_csv_pipeline,
        "raw_data_from_api": raw_data_from_api_pipeline,
        "raw_data_sensors": raw_data_sensors_pipeline,
        "pm25": pm25_pipeline
    }

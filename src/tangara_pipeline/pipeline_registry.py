"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline, pipeline

from tangara_pipeline.pipelines import pm25
from tangara_pipeline.pipelines import pm25_nowcast_aqi


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    pm25_pipeline = pm25.create_pipeline()
    pm25_nowcast_aqi_pipeline = pm25_nowcast_aqi.create_pipeline()

    return {
        "__default__": pm25_pipeline,
        "pm25": pm25_pipeline,
        "pm25_nowcast_aqi": pm25_nowcast_aqi_pipeline
    }

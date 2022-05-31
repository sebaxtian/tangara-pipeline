"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline, pipeline

from tangara_pipeline.pipelines import raw_data_from_csv


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    raw_data_from_csv_pipeline = raw_data_from_csv.create_pipeline()

    return {
        "__default__": raw_data_from_csv_pipeline,
        "raw_data_from_csv": raw_data_from_csv_pipeline
    }

"""
This module contains an example test.

Tests should be placed in ``src/tests``, in modules that mirror your
project's structure, and in files named test_*.py. They are simply functions
named ``test_*`` which test a unit of logic.

To run the tests, run ``kedro test`` from the project root directory.
"""
from pathlib import Path

import pytest

from kedro.framework.project import settings
from kedro.config import ConfigLoader
from kedro.framework.context import KedroContext
from kedro.framework.hooks import _create_hook_manager

from .fixtures.tangaras_fixture import tangaras_fixture


@pytest.fixture(scope="module")
def config_loader():
    return ConfigLoader(conf_source=str(Path.cwd() / settings.CONF_SOURCE))


@pytest.fixture(scope="module")
def project_context(config_loader):
    return KedroContext(
        package_name="tangara_pipeline",
        project_path=Path.cwd(),
        config_loader=config_loader,
        hook_manager=_create_hook_manager(),
    )


# The tests below are here for the demonstration purpose
# and should be replaced with the ones testing the project
# functionality
class TestProjectContext:
    def test_project_path(self, project_context):
        assert project_context.project_path == Path.cwd()

    def test_project_catalog(self, project_context):
        base_catalog = ["tangaras"]
        assert set(base_catalog).issubset(set(project_context.catalog.list()))

    def test_project_params(self, project_context):
        base_params = ["raw_data_origin", "start_datetime", "end_datetime"]
        assert set(set(project_context.params.keys())).issubset(base_params)

    def test_tangaras_fixture(self, tangaras_fixture):
        assert tangaras_fixture.columns.to_list() == [
            "ID",
            "GEOHASH",
            "MAC",
            "GEOLOCATION",
            "LATITUDE",
            "LONGITUDE",
            "DATETIME",
        ]

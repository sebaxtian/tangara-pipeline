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
from .fixtures.pm25_raw_fixture import pm25_raw_fixture


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
        base_catalog = [
            "tangara_stations",
            "pm25_raw",
            "pm25_clean",
            "pm25_last_hour",
            "pm25_last_8h",
            "pm25_last_12h",
            "pm25_last_24h",
            "aqi_instant",
            "aqi_last_hour",
            "aqi_last_8h",
            "aqi_last_12h",
            "aqi_last_24h",
            "stations_measurements",
        ]
        assert set(base_catalog).issubset(set(project_context.catalog.list()))

    def test_project_params(self, project_context):
        base_params = [
            "start_datetime",
            "end_datetime",
            "nowcast_datetime",
            "influxdb_version",
        ]
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
    
    def test_pm25_raw_fixture(self, pm25_raw_fixture):
        assert pm25_raw_fixture.columns.to_list() == [
            "DATETIME",
            "TANGARA_2BBA",
            "TANGARA_14D6",
            "TANGARA_1CE2",
            "TANGARA_1FCA",
            "TANGARA_2492",
            "TANGARA_2FF6",
            "TANGARA_48C6",
        ]

#  Copyright 2026 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Tests for the Superset 5.0+ position_json fallback in
SupersetSourceMixin._get_charts_of_dashboard
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from metadata.ingestion.source.dashboard.superset.mixin import SupersetSourceMixin
from metadata.ingestion.source.dashboard.superset.models import (
    DashboardResult,
    FetchDashboard,
    FetchedDashboard,
)


def _get_charts_of_dashboard(client, dashboard):
    """Call the mixin method unbound: the class is abstract, and the method
    only relies on self.client"""
    stub_self = SimpleNamespace(client=client)
    return SupersetSourceMixin._get_charts_of_dashboard(stub_self, dashboard)


def test_db_mode_engine_without_position_json_does_not_call_api_fallback(caplog):
    """
    In DB mode self.client is a SQLAlchemy Engine, which has no
    fetch_dashboard method. The API fallback must be skipped instead of
    raising `'Engine' object has no attribute 'fetch_dashboard'`.
    """
    engine = MagicMock(spec=["connect", "execute"])
    dashboard = FetchDashboard(id=464, position_json=None)

    result = _get_charts_of_dashboard(engine, dashboard)

    assert result == []
    assert not any(
        "Failed to charts of dashboard" in record.message for record in caplog.records
    )


def test_api_mode_fallback_fetches_dashboard_details():
    client = MagicMock(spec=["fetch_dashboard"])
    client.fetch_dashboard.return_value = FetchedDashboard(
        id=14,
        result=DashboardResult(
            position_json='{"CHART-abc": {"meta": {"chartId": 69}}}'
        ),
    )
    dashboard = FetchDashboard(id=14, position_json=None)

    result = _get_charts_of_dashboard(client, dashboard)

    client.fetch_dashboard.assert_called_once_with(14)
    assert result == [69]


def test_position_json_present_skips_fallback():
    client = MagicMock(spec=["fetch_dashboard"])
    dashboard = FetchDashboard(
        id=7, position_json='{"CHART-abc": {"meta": {"chartId": 42}}}'
    )

    result = _get_charts_of_dashboard(client, dashboard)

    client.fetch_dashboard.assert_not_called()
    assert result == [42]

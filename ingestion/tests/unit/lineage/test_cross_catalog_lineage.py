#  Copyright 2025 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Unit tests for the cross-catalog (database-wildcard) fallback in
get_table_entities_from_query.

A view in one catalog (e.g. `hiyu`) can reference a table that was ingested
under a different database name in another service (e.g.
`trino_hiyu.iceberg.<schema>.<table>`). The exact and schema fallbacks search
with the view's database, miss it, and the run produced no lineage. The
database=None fallback wildcards the database so the table resolves across the
configured cross-database services.
"""

from unittest.mock import MagicMock, patch

from metadata.ingestion.lineage.sql_lineage import get_table_entities_from_query


class TestCrossCatalogFallback:
    """get_table_entities_from_query must fall back to a wildcard database."""

    def _run(self, side_effect):
        metadata = MagicMock()
        with patch(
            "metadata.ingestion.lineage.sql_lineage.search_table_entities",
            side_effect=side_effect,
        ) as mock_search:
            result = get_table_entities_from_query(
                metadata=metadata,
                service_names=["trino_views_hiyu", "trino_hiyu"],
                database_name="hiyu",
                database_schema="e2e_test",
                table_name="test_current_date",
                schema_fallback=True,
            )
        return result, mock_search

    def test_database_wildcard_fallback_resolves_cross_catalog(self):
        """When exact + schema searches (db=hiyu) miss, retry with db=None."""
        sentinel_table = MagicMock()

        def side_effect(*_, database=None, **__):
            # Only the database-wildcard search resolves the table.
            return [sentinel_table] if database is None else None

        result, mock_search = self._run(side_effect)

        assert result == [sentinel_table]
        # At least one search must have used database=None (the wildcard fallback).
        assert any(
            call.kwargs.get("database") is None for call in mock_search.call_args_list
        )

    def test_returns_none_when_table_truly_absent(self):
        """If no search ever resolves, the function still returns None."""
        result, _ = self._run(lambda *a, **k: None)

        assert result is None

    def test_exact_match_skips_fallbacks(self):
        """A first-try hit (db=hiyu) returns without any wildcard search."""
        sentinel_table = MagicMock()
        result, mock_search = self._run(lambda *a, **k: [sentinel_table])

        assert result == [sentinel_table]
        # Only the first (exact) search runs; no fallback needed.
        assert mock_search.call_count == 1

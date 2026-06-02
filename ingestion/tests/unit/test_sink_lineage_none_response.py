#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");

"""
Unit tests for sink-level handling of a None response from add_lineage in
write_lineage.

add_lineage PUTs the edge first and only then fetches the source entity
lineage back. When that follow-up GET 404s (e.g. the source has a dangling
downstream reference to a deleted entity), add_lineage returns None even
though the edge was written. The sink must not crash on that None.
"""

from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type.entityLineage import EntitiesEdge
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.sink.metadata_rest import (
    MetadataRestSink,
    MetadataRestSinkConfig,
)


class TestSinkLineageNoneResponse:
    """write_lineage must tolerate a None response from add_lineage"""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.mock_metadata = Mock()
        self.config = MetadataRestSinkConfig(bulk_sink_batch_size=10)
        self.sink = MetadataRestSink(self.config, self.mock_metadata)

    def _lineage_request(self) -> AddLineageRequest:
        return AddLineageRequest(
            edge=EntitiesEdge(
                fromEntity=EntityReference(id=uuid4(), type="dashboardDataModel"),
                toEntity=EntityReference(id=uuid4(), type="chart"),
            )
        )

    @patch("metadata.ingestion.sink.metadata_rest.logger")
    def test_none_response_does_not_crash(self, mock_logger):
        """A None add_lineage response is logged and yields a non-error result"""
        self.mock_metadata.add_lineage.return_value = None

        result = self.sink.write_lineage(self._lineage_request())

        assert result.left is None
        assert result.right is None
        mock_logger.warning.assert_called_once()

    def test_error_response_is_propagated(self):
        """An error dict from add_lineage is still surfaced as a failure"""
        self.mock_metadata.add_lineage.return_value = {"error": "boom"}

        result = self.sink.write_lineage(self._lineage_request())

        assert result.right is None
        assert result.left is not None
        assert result.left.error == "boom"

    def test_success_response_returns_fqn(self):
        """A successful response returns the source entity FQN"""
        self.mock_metadata.add_lineage.return_value = {
            "entity": {"fullyQualifiedName": "service.model"}
        }

        result = self.sink.write_lineage(self._lineage_request())

        assert result.left is None
        assert result.right == "service.model"

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
MetadataRestSink.write_lineage must not crash when add_lineage returns None,
which happens when the edge is written but reading back the origin node's
lineage fails (e.g. 404 because its graph references a deleted entity).
"""

from unittest.mock import MagicMock

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type.entityLineage import EntitiesEdge
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.sink.metadata_rest import (
    MetadataRestSink,
    MetadataRestSinkConfig,
)

FROM_ID = "ab8f49d7-afba-4164-ba06-a0dc6300c5d2"
TO_ID = "615ff273-1180-4fc2-8682-b5e4a646a6fe"

LINEAGE_REQUEST = AddLineageRequest(
    edge=EntitiesEdge(
        fromEntity=EntityReference(id=FROM_ID, type="dashboardDataModel"),
        toEntity=EntityReference(id=TO_ID, type="chart"),
    )
)


def _sink_with_add_lineage_returning(value) -> MetadataRestSink:
    metadata = MagicMock()
    metadata.add_lineage.return_value = value
    return MetadataRestSink(config=MetadataRestSinkConfig(), metadata=metadata)


def test_write_lineage_handles_none_response():
    sink = _sink_with_add_lineage_returning(None)

    result = sink.write_lineage(LINEAGE_REQUEST)

    assert result.left is None
    assert result.right == FROM_ID


def test_write_lineage_error_response_is_reported():
    sink = _sink_with_add_lineage_returning({"error": "Error 400 trying to PUT"})

    result = sink.write_lineage(LINEAGE_REQUEST)

    assert result.left is not None
    assert "Error 400" in result.left.error


def test_write_lineage_success_returns_entity_fqn():
    sink = _sink_with_add_lineage_returning(
        {"entity": {"fullyQualifiedName": "service.model_1"}}
    )

    result = sink.write_lineage(LINEAGE_REQUEST)

    assert result.left is None
    assert result.right == "service.model_1"

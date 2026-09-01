"""Public data shapes returned by, or passed into, RociaDbClient calls.

Page options, upload options, and a document query's filters/sort are keyword-only
parameters on the relevant `RociaDbClient` method rather than their own "options bag"
type here: Python's native keyword arguments already give the readability such a bag
exists to provide.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, List, Optional, Sequence, TypeVar

#: Shared by every read method that JSON-decodes a payload into a caller-supplied type
#: via an optional `decoder` keyword argument.
T = TypeVar("T")


@dataclass(frozen=True)
class Page(Generic[T]):
    """One page of a cursor-paginated listing.

    `next_cursor` is `None` exactly when there is no further page. An empty, or
    shorter-than-`limit`, `items` list can still occur mid-listing (e.g. an index entry
    surviving a deleted document) - never treat that alone as the end.
    """

    items: List[T]
    next_cursor: Optional[str]


@dataclass(frozen=True)
class DocumentPage(Page[T]):
    """One page of documents, additionally carrying the total match count.

    The cost of `total_count` varies by RPC: free on `list_documents`, an index count on
    `find_documents_by_field`, expensive on `query_documents` - never loop on it just to
    obtain a count.
    """

    total_count: int


@dataclass(frozen=True)
class CollectionInfo:
    """One document collection together with the number of documents it holds."""

    collection: str
    count: int


@dataclass(frozen=True)
class Neighbor:
    """A raw graph neighbor: the node id it points to and the edge id that reaches it."""

    node_id: str
    edge_id: str


@dataclass(frozen=True)
class NeighborNode(Neighbor, Generic[T]):
    """A graph neighbor together with its decoded node JSON payload."""

    value: T


@dataclass(frozen=True)
class NodeInput:
    """One node to upsert in a `RociaDbClient.put_nodes` batch.

    `value` is write-only JSON (`Any`, not the generic `T` used by read methods): a
    batch upsert never decodes it back. `request_id` is the idempotency key for this
    item's `PutNode` call; when omitted, one is generated automatically. Provide it
    explicitly, and reuse the same value on a retry, so a batch replayed after a timeout
    resumes safely - the server deduplicates on `(tenant, operation, request_id)`.
    """

    node_id: str
    value: Any
    request_id: Optional[str] = None


@dataclass(frozen=True)
class EdgeInput:
    """One edge to upsert in a `RociaDbClient.add_edges` batch.

    `from_id`/`to_id` carry the wire's `from`/`to` fields: `from` is a reserved word in
    Python and cannot be a field name. See `NodeInput.request_id` for why reusing
    `request_id` on a retry matters.
    """

    edge_id: str
    from_id: str
    to_id: str
    label: str
    value: Any
    request_id: Optional[str] = None


class DocumentQueryOperator(str, Enum):
    """`QueryDoc` filter operator.

    `CONTAINS` ignores a filter term shorter than 3 characters at the index level - the
    server rejects a query left with no indexable filter at all.
    """

    EQ = "eq"
    IN = "in"
    CONTAINS = "contains"


class DocumentSortDirection(str, Enum):
    """`QueryDoc` sort direction."""

    ASC = "asc"
    DESC = "desc"


@dataclass(frozen=True)
class DocumentQueryFilter:
    """One `QueryDoc` filter. Every filter passed to a query is ANDed together."""

    field: str
    operator: DocumentQueryOperator
    values: Sequence[Any]


@dataclass(frozen=True)
class DocumentQuerySort:
    """One `QueryDoc` sort level, applied in the order given."""

    field: str
    direction: DocumentSortDirection


@dataclass(frozen=True)
class FileMetadata:
    """File metadata returned by `RociaDbClient.stat_file`."""

    size_bytes: int
    content_type: str
    checksum: bytes
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class RawUploadMessage:
    """One message of the raw, caller-driven `RociaDbClient.upload_file_raw` stream.

    Every field travels on the wire exactly as given, for every message: there is no
    assisted re-chunking and no first-message/later-message distinction. The first
    message must carry `tenant_id`, `bucket`, `file_id`, the exact total `size_bytes`,
    and a 32-byte SHA-256 `checksum`; `chunk` must be at most 1 MiB; metadata fields on
    later messages are ignored by the server and may be left empty.
    """

    tenant_id: str
    bucket: str
    file_id: str
    size_bytes: int
    content_type: str
    checksum: bytes
    chunk: bytes
    request_id: str

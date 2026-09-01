"""Python client SDK for RociaDB's document, graph, file, and tenant gRPC services.

Build a connected client with `RociaDbBuilder` or the `RociaDbClient.connect`
classmethod; both are asynchronous and built on `grpc.aio`.
"""

from __future__ import annotations

from rociadb_sdk._pagination import DEFAULT_PAGE_SIZE
from rociadb_sdk.client import (
    CONCURRENT_REQUESTS,
    DEFAULT_CONNECT_TIMEOUT,
    RociaDbBuilder,
    RociaDbClient,
)
from rociadb_sdk.errors import (
    RociaDbAuthError,
    RociaDbConnectionError,
    RociaDbDecodeError,
    RociaDbEncodeError,
    RociaDbError,
    RociaDbStatusError,
    RociaDbValidationError,
)
from rociadb_sdk.file import CHECKSUM_LEN, MAX_FILE_BYTES, UPLOAD_CHUNK_BYTES
from rociadb_sdk.types import (
    CollectionInfo,
    DocumentPage,
    DocumentQueryFilter,
    DocumentQueryOperator,
    DocumentQuerySort,
    DocumentSortDirection,
    EdgeInput,
    FileMetadata,
    Neighbor,
    NeighborNode,
    NodeInput,
    Page,
    RawUploadMessage,
)

__all__ = [
    "CHECKSUM_LEN",
    "CONCURRENT_REQUESTS",
    "DEFAULT_CONNECT_TIMEOUT",
    "DEFAULT_PAGE_SIZE",
    "MAX_FILE_BYTES",
    "UPLOAD_CHUNK_BYTES",
    "CollectionInfo",
    "DocumentPage",
    "DocumentQueryFilter",
    "DocumentQueryOperator",
    "DocumentQuerySort",
    "DocumentSortDirection",
    "EdgeInput",
    "FileMetadata",
    "Neighbor",
    "NeighborNode",
    "NodeInput",
    "Page",
    "RawUploadMessage",
    "RociaDbAuthError",
    "RociaDbBuilder",
    "RociaDbClient",
    "RociaDbConnectionError",
    "RociaDbDecodeError",
    "RociaDbEncodeError",
    "RociaDbError",
    "RociaDbStatusError",
    "RociaDbValidationError",
]

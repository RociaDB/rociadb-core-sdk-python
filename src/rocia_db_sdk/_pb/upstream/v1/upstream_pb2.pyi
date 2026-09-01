from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QueryOperator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    QUERY_OPERATOR_UNSPECIFIED: _ClassVar[QueryOperator]
    QUERY_OPERATOR_EQ: _ClassVar[QueryOperator]
    QUERY_OPERATOR_IN: _ClassVar[QueryOperator]
    QUERY_OPERATOR_CONTAINS: _ClassVar[QueryOperator]

class SortDirection(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SORT_DIRECTION_UNSPECIFIED: _ClassVar[SortDirection]
    SORT_DIRECTION_ASC: _ClassVar[SortDirection]
    SORT_DIRECTION_DESC: _ClassVar[SortDirection]

QUERY_OPERATOR_UNSPECIFIED: QueryOperator
QUERY_OPERATOR_EQ: QueryOperator
QUERY_OPERATOR_IN: QueryOperator
QUERY_OPERATOR_CONTAINS: QueryOperator
SORT_DIRECTION_UNSPECIFIED: SortDirection
SORT_DIRECTION_ASC: SortDirection
SORT_DIRECTION_DESC: SortDirection

class PageRequest(_message.Message):
    __slots__ = ("limit", "cursor")
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    limit: int
    cursor: str
    def __init__(self, limit: _Optional[int] = ..., cursor: _Optional[str] = ...) -> None: ...

class PageResponse(_message.Message):
    __slots__ = ("next_cursor",)
    NEXT_CURSOR_FIELD_NUMBER: _ClassVar[int]
    next_cursor: str
    def __init__(self, next_cursor: _Optional[str] = ...) -> None: ...

class PutDocRequest(_message.Message):
    __slots__ = ("tenant_id", "collection", "id", "json", "request_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    JSON_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    collection: str
    id: str
    json: bytes
    request_id: str
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        collection: _Optional[str] = ...,
        id: _Optional[str] = ...,
        json: _Optional[bytes] = ...,
        request_id: _Optional[str] = ...,
    ) -> None: ...

class GetDocRequest(_message.Message):
    __slots__ = ("tenant_id", "collection", "id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    collection: str
    id: str
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        collection: _Optional[str] = ...,
        id: _Optional[str] = ...,
    ) -> None: ...

class GetDocResponse(_message.Message):
    __slots__ = ("json",)
    JSON_FIELD_NUMBER: _ClassVar[int]
    json: bytes
    def __init__(self, json: _Optional[bytes] = ...) -> None: ...

class DeleteDocRequest(_message.Message):
    __slots__ = ("tenant_id", "collection", "id", "request_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    collection: str
    id: str
    request_id: str
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        collection: _Optional[str] = ...,
        id: _Optional[str] = ...,
        request_id: _Optional[str] = ...,
    ) -> None: ...

class FindByFieldRequest(_message.Message):
    __slots__ = ("tenant_id", "collection", "field", "value_json", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    VALUE_JSON_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    collection: str
    field: str
    value_json: bytes
    page: PageRequest
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        collection: _Optional[str] = ...,
        field: _Optional[str] = ...,
        value_json: _Optional[bytes] = ...,
        page: _Optional[_Union[PageRequest, _Mapping]] = ...,
    ) -> None: ...

class ListDocRequest(_message.Message):
    __slots__ = ("tenant_id", "collection", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    collection: str
    page: PageRequest
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        collection: _Optional[str] = ...,
        page: _Optional[_Union[PageRequest, _Mapping]] = ...,
    ) -> None: ...

class QueryFilter(_message.Message):
    __slots__ = ("field", "operator", "values_json")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_FIELD_NUMBER: _ClassVar[int]
    VALUES_JSON_FIELD_NUMBER: _ClassVar[int]
    field: str
    operator: QueryOperator
    values_json: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(
        self,
        field: _Optional[str] = ...,
        operator: _Optional[_Union[QueryOperator, str]] = ...,
        values_json: _Optional[_Iterable[bytes]] = ...,
    ) -> None: ...

class QuerySort(_message.Message):
    __slots__ = ("field", "direction")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    DIRECTION_FIELD_NUMBER: _ClassVar[int]
    field: str
    direction: SortDirection
    def __init__(
        self, field: _Optional[str] = ..., direction: _Optional[_Union[SortDirection, str]] = ...
    ) -> None: ...

class QueryDocRequest(_message.Message):
    __slots__ = ("tenant_id", "collection", "filters", "sort", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    SORT_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    collection: str
    filters: _containers.RepeatedCompositeFieldContainer[QueryFilter]
    sort: _containers.RepeatedCompositeFieldContainer[QuerySort]
    page: PageRequest
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        collection: _Optional[str] = ...,
        filters: _Optional[_Iterable[_Union[QueryFilter, _Mapping]]] = ...,
        sort: _Optional[_Iterable[_Union[QuerySort, _Mapping]]] = ...,
        page: _Optional[_Union[PageRequest, _Mapping]] = ...,
    ) -> None: ...

class FindByFieldResponse(_message.Message):
    __slots__ = ("json", "page", "total_count")
    JSON_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    json: _containers.RepeatedScalarFieldContainer[bytes]
    page: PageResponse
    total_count: int
    def __init__(
        self,
        json: _Optional[_Iterable[bytes]] = ...,
        page: _Optional[_Union[PageResponse, _Mapping]] = ...,
        total_count: _Optional[int] = ...,
    ) -> None: ...

class ListDocResponse(_message.Message):
    __slots__ = ("json", "page", "total_count")
    JSON_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    json: _containers.RepeatedScalarFieldContainer[bytes]
    page: PageResponse
    total_count: int
    def __init__(
        self,
        json: _Optional[_Iterable[bytes]] = ...,
        page: _Optional[_Union[PageResponse, _Mapping]] = ...,
        total_count: _Optional[int] = ...,
    ) -> None: ...

class ListCollectionsRequest(_message.Message):
    __slots__ = ("tenant_id", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    page: PageRequest
    def __init__(
        self, tenant_id: _Optional[str] = ..., page: _Optional[_Union[PageRequest, _Mapping]] = ...
    ) -> None: ...

class CollectionInfo(_message.Message):
    __slots__ = ("collection", "count")
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    collection: str
    count: int
    def __init__(self, collection: _Optional[str] = ..., count: _Optional[int] = ...) -> None: ...

class ListCollectionsResponse(_message.Message):
    __slots__ = ("collections", "page")
    COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    collections: _containers.RepeatedCompositeFieldContainer[CollectionInfo]
    page: PageResponse
    def __init__(
        self,
        collections: _Optional[_Iterable[_Union[CollectionInfo, _Mapping]]] = ...,
        page: _Optional[_Union[PageResponse, _Mapping]] = ...,
    ) -> None: ...

class ListBucketsRequest(_message.Message):
    __slots__ = ("tenant_id", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    page: PageRequest
    def __init__(
        self, tenant_id: _Optional[str] = ..., page: _Optional[_Union[PageRequest, _Mapping]] = ...
    ) -> None: ...

class ListBucketsResponse(_message.Message):
    __slots__ = ("buckets", "page")
    BUCKETS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    buckets: _containers.RepeatedScalarFieldContainer[str]
    page: PageResponse
    def __init__(
        self,
        buckets: _Optional[_Iterable[str]] = ...,
        page: _Optional[_Union[PageResponse, _Mapping]] = ...,
    ) -> None: ...

class ListFilesRequest(_message.Message):
    __slots__ = ("tenant_id", "bucket", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    bucket: str
    page: PageRequest
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        bucket: _Optional[str] = ...,
        page: _Optional[_Union[PageRequest, _Mapping]] = ...,
    ) -> None: ...

class ListFilesResponse(_message.Message):
    __slots__ = ("file_ids", "page")
    FILE_IDS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    file_ids: _containers.RepeatedScalarFieldContainer[str]
    page: PageResponse
    def __init__(
        self,
        file_ids: _Optional[_Iterable[str]] = ...,
        page: _Optional[_Union[PageResponse, _Mapping]] = ...,
    ) -> None: ...

class ListGraphsRequest(_message.Message):
    __slots__ = ("tenant_id", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    page: PageRequest
    def __init__(
        self, tenant_id: _Optional[str] = ..., page: _Optional[_Union[PageRequest, _Mapping]] = ...
    ) -> None: ...

class ListGraphsResponse(_message.Message):
    __slots__ = ("graphs", "page")
    GRAPHS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    graphs: _containers.RepeatedScalarFieldContainer[str]
    page: PageResponse
    def __init__(
        self,
        graphs: _Optional[_Iterable[str]] = ...,
        page: _Optional[_Union[PageResponse, _Mapping]] = ...,
    ) -> None: ...

class ListNodesRequest(_message.Message):
    __slots__ = ("tenant_id", "graph", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    graph: str
    page: PageRequest
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        graph: _Optional[str] = ...,
        page: _Optional[_Union[PageRequest, _Mapping]] = ...,
    ) -> None: ...

class ListNodesResponse(_message.Message):
    __slots__ = ("node_ids", "page")
    NODE_IDS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    node_ids: _containers.RepeatedScalarFieldContainer[str]
    page: PageResponse
    def __init__(
        self,
        node_ids: _Optional[_Iterable[str]] = ...,
        page: _Optional[_Union[PageResponse, _Mapping]] = ...,
    ) -> None: ...

class ListTenantsRequest(_message.Message):
    __slots__ = ("page",)
    PAGE_FIELD_NUMBER: _ClassVar[int]
    page: PageRequest
    def __init__(self, page: _Optional[_Union[PageRequest, _Mapping]] = ...) -> None: ...

class ListTenantsResponse(_message.Message):
    __slots__ = ("tenant_ids", "page")
    TENANT_IDS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_ids: _containers.RepeatedScalarFieldContainer[str]
    page: PageResponse
    def __init__(
        self,
        tenant_ids: _Optional[_Iterable[str]] = ...,
        page: _Optional[_Union[PageResponse, _Mapping]] = ...,
    ) -> None: ...

class QueryDocResponse(_message.Message):
    __slots__ = ("json", "page", "total_count")
    JSON_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    json: _containers.RepeatedScalarFieldContainer[bytes]
    page: PageResponse
    total_count: int
    def __init__(
        self,
        json: _Optional[_Iterable[bytes]] = ...,
        page: _Optional[_Union[PageResponse, _Mapping]] = ...,
        total_count: _Optional[int] = ...,
    ) -> None: ...

class PutNodeRequest(_message.Message):
    __slots__ = ("tenant_id", "graph", "node_id", "json", "request_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    JSON_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    graph: str
    node_id: str
    json: bytes
    request_id: str
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        graph: _Optional[str] = ...,
        node_id: _Optional[str] = ...,
        json: _Optional[bytes] = ...,
        request_id: _Optional[str] = ...,
    ) -> None: ...

class GetNodeRequest(_message.Message):
    __slots__ = ("tenant_id", "graph", "node_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    graph: str
    node_id: str
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        graph: _Optional[str] = ...,
        node_id: _Optional[str] = ...,
    ) -> None: ...

class GetNodeResponse(_message.Message):
    __slots__ = ("json",)
    JSON_FIELD_NUMBER: _ClassVar[int]
    json: bytes
    def __init__(self, json: _Optional[bytes] = ...) -> None: ...

class AddEdgeRequest(_message.Message):
    __slots__ = ("tenant_id", "graph", "edge_id", "to", "label", "json", "request_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    EDGE_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    JSON_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    graph: str
    edge_id: str
    to: str
    label: str
    json: bytes
    request_id: str
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        graph: _Optional[str] = ...,
        edge_id: _Optional[str] = ...,
        to: _Optional[str] = ...,
        label: _Optional[str] = ...,
        json: _Optional[bytes] = ...,
        request_id: _Optional[str] = ...,
        **kwargs,
    ) -> None: ...

class DeleteEdgeRequest(_message.Message):
    __slots__ = ("tenant_id", "graph", "edge_id", "request_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    EDGE_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    graph: str
    edge_id: str
    request_id: str
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        graph: _Optional[str] = ...,
        edge_id: _Optional[str] = ...,
        request_id: _Optional[str] = ...,
    ) -> None: ...

class NeighborsOutRequest(_message.Message):
    __slots__ = ("tenant_id", "graph", "label", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    FROM_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    graph: str
    label: str
    page: PageRequest
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        graph: _Optional[str] = ...,
        label: _Optional[str] = ...,
        page: _Optional[_Union[PageRequest, _Mapping]] = ...,
        **kwargs,
    ) -> None: ...

class Neighbor(_message.Message):
    __slots__ = ("node_id", "edge_id")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    EDGE_ID_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    edge_id: str
    def __init__(self, node_id: _Optional[str] = ..., edge_id: _Optional[str] = ...) -> None: ...

class NeighborsOutResponse(_message.Message):
    __slots__ = ("neighbors", "page")
    NEIGHBORS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    neighbors: _containers.RepeatedCompositeFieldContainer[Neighbor]
    page: PageResponse
    def __init__(
        self,
        neighbors: _Optional[_Iterable[_Union[Neighbor, _Mapping]]] = ...,
        page: _Optional[_Union[PageResponse, _Mapping]] = ...,
    ) -> None: ...

class NeighborsInRequest(_message.Message):
    __slots__ = ("tenant_id", "graph", "to", "label", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    graph: str
    to: str
    label: str
    page: PageRequest
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        graph: _Optional[str] = ...,
        to: _Optional[str] = ...,
        label: _Optional[str] = ...,
        page: _Optional[_Union[PageRequest, _Mapping]] = ...,
    ) -> None: ...

class NeighborsInResponse(_message.Message):
    __slots__ = ("neighbors", "page")
    NEIGHBORS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    neighbors: _containers.RepeatedCompositeFieldContainer[Neighbor]
    page: PageResponse
    def __init__(
        self,
        neighbors: _Optional[_Iterable[_Union[Neighbor, _Mapping]]] = ...,
        page: _Optional[_Union[PageResponse, _Mapping]] = ...,
    ) -> None: ...

class UploadRequest(_message.Message):
    __slots__ = (
        "tenant_id",
        "bucket",
        "file_id",
        "size_bytes",
        "content_type",
        "checksum",
        "chunk",
        "request_id",
    )
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    CHUNK_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    bucket: str
    file_id: str
    size_bytes: int
    content_type: str
    checksum: bytes
    chunk: bytes
    request_id: str
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        bucket: _Optional[str] = ...,
        file_id: _Optional[str] = ...,
        size_bytes: _Optional[int] = ...,
        content_type: _Optional[str] = ...,
        checksum: _Optional[bytes] = ...,
        chunk: _Optional[bytes] = ...,
        request_id: _Optional[str] = ...,
    ) -> None: ...

class DownloadRequest(_message.Message):
    __slots__ = ("tenant_id", "bucket", "file_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    bucket: str
    file_id: str
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        bucket: _Optional[str] = ...,
        file_id: _Optional[str] = ...,
    ) -> None: ...

class DownloadResponse(_message.Message):
    __slots__ = ("chunk",)
    CHUNK_FIELD_NUMBER: _ClassVar[int]
    chunk: bytes
    def __init__(self, chunk: _Optional[bytes] = ...) -> None: ...

class StatRequest(_message.Message):
    __slots__ = ("tenant_id", "bucket", "file_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    bucket: str
    file_id: str
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        bucket: _Optional[str] = ...,
        file_id: _Optional[str] = ...,
    ) -> None: ...

class StatResponse(_message.Message):
    __slots__ = ("size_bytes", "content_type", "checksum", "created_at", "updated_at")
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    size_bytes: int
    content_type: str
    checksum: bytes
    created_at: str
    updated_at: str
    def __init__(
        self,
        size_bytes: _Optional[int] = ...,
        content_type: _Optional[str] = ...,
        checksum: _Optional[bytes] = ...,
        created_at: _Optional[str] = ...,
        updated_at: _Optional[str] = ...,
    ) -> None: ...

class DeleteRequest(_message.Message):
    __slots__ = ("tenant_id", "bucket", "file_id", "request_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    bucket: str
    file_id: str
    request_id: str
    def __init__(
        self,
        tenant_id: _Optional[str] = ...,
        bucket: _Optional[str] = ...,
        file_id: _Optional[str] = ...,
        request_id: _Optional[str] = ...,
    ) -> None: ...

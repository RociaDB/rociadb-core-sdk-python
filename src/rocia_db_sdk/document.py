"""DocumentService RPC methods: create, read, delete, search, list, and query documents."""

from __future__ import annotations

from typing import Any, Callable, Optional, Sequence, TypeVar, overload
from uuid import uuid4

from rocia_db_sdk._pagination import _optional_cursor, _page_request
from rocia_db_sdk._pb.upstream.v1 import upstream_pb2 as pb
from rocia_db_sdk._pb.upstream.v1 import upstream_pb2_grpc as rpc
from rocia_db_sdk.errors import RociaDbValidationError, _call, _decode_json, _encode_json
from rocia_db_sdk.types import (
    CollectionInfo,
    DocumentPage,
    DocumentQueryFilter,
    DocumentQueryOperator,
    DocumentQuerySort,
    DocumentSortDirection,
    Page,
)

T = TypeVar("T")

_QUERY_OPERATOR_TO_PROTO = {
    DocumentQueryOperator.EQ: pb.QUERY_OPERATOR_EQ,
    DocumentQueryOperator.IN: pb.QUERY_OPERATOR_IN,
    DocumentQueryOperator.CONTAINS: pb.QUERY_OPERATOR_CONTAINS,
}
_SORT_DIRECTION_TO_PROTO = {
    DocumentSortDirection.ASC: pb.SORT_DIRECTION_ASC,
    DocumentSortDirection.DESC: pb.SORT_DIRECTION_DESC,
}


class _DocumentMixin:
    _documents: rpc.DocumentServiceStub
    _graph: rpc.GraphServiceStub

    async def put_document(
        self,
        tenant_id: str,
        collection: str,
        document_id: str,
        value: Any,
        *,
        request_id: Optional[str] = None,
    ) -> None:
        """Create or replace one JSON document.

        `request_id` defaults to ``f"put_document:{collection}:{uuid4()}"`` when
        omitted; reuse the same value on a retry so the server recognizes a repeated
        write instead of applying it twice.
        """
        await _call(
            "PutDoc",
            self._documents.PutDoc(
                pb.PutDocRequest(
                    tenant_id=tenant_id,
                    collection=collection,
                    id=document_id,
                    json=_encode_json(value, "document json"),
                    request_id=request_id
                    if request_id is not None
                    else f"put_document:{collection}:{uuid4()}",
                )
            ),
        )

    async def create_document(
        self,
        tenant_id: str,
        collection: str,
        document_id: str,
        value: Any,
        *,
        node_label: Optional[str] = None,
        node_graph: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> None:
        """Create or replace a document and, optionally, a `label:id` graph node
        pointing to it.

        `node_label` and `node_graph` must be given together: if only one is set, this
        raises `RociaDbValidationError` before any network call. Not atomic - the
        document is written first and the node binding second, using its own
        auto-generated idempotency key - so if the node write fails, the document is
        left in place without its binding.
        """
        if (node_label is None) != (node_graph is None):
            raise RociaDbValidationError(
                "node_label and node_graph must be provided together "
                f"(got node_label={node_label!r}, node_graph={node_graph!r})"
            )
        await self.put_document(
            tenant_id,
            collection,
            document_id,
            value,
            request_id=request_id
            if request_id is not None
            else f"put_document:{collection}:{uuid4()}",
        )
        if node_label is not None and node_graph is not None:
            await _call(
                "PutNode",
                self._graph.PutNode(
                    pb.PutNodeRequest(
                        tenant_id=tenant_id,
                        graph=node_graph,
                        node_id=f"{node_label}:{document_id}",
                        json=_encode_json(
                            {"collection": collection, "id": document_id}, "node json"
                        ),
                        request_id=f"put_node:{uuid4()}",
                    )
                ),
            )

    async def delete_document(
        self,
        tenant_id: str,
        collection: str,
        document_id: str,
        *,
        request_id: Optional[str] = None,
    ) -> None:
        """Delete one document. Idempotent server-side: deleting an already-deleted or
        never-existing document is not an error.
        """
        await _call(
            "DeleteDoc",
            self._documents.DeleteDoc(
                pb.DeleteDocRequest(
                    tenant_id=tenant_id,
                    collection=collection,
                    id=document_id,
                    request_id=request_id
                    if request_id is not None
                    else f"delete_document:{collection}:{uuid4()}",
                )
            ),
        )

    @overload
    async def get_document(self, tenant_id: str, collection: str, document_id: str) -> Any: ...
    @overload
    async def get_document(
        self,
        tenant_id: str,
        collection: str,
        document_id: str,
        *,
        decoder: Callable[[Any], T],
    ) -> T: ...
    async def get_document(
        self,
        tenant_id: str,
        collection: str,
        document_id: str,
        *,
        decoder: Optional[Callable[[Any], T]] = None,
    ) -> Any:
        """Fetch and JSON-decode one document.

        Without `decoder`, the decoded JSON value is returned as-is. With `decoder`,
        its return value is returned instead - Python has no reified generic type
        parameter to select a return type at the call site.
        """
        response = await _call(
            "GetDoc",
            self._documents.GetDoc(
                pb.GetDocRequest(tenant_id=tenant_id, collection=collection, id=document_id)
            ),
        )
        value = _decode_json(response.json, "document")
        return decoder(value) if decoder is not None else value

    @overload
    async def find_documents_by_field(
        self,
        tenant_id: str,
        collection: str,
        field: str,
        value: Any,
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> DocumentPage[Any]: ...
    @overload
    async def find_documents_by_field(
        self,
        tenant_id: str,
        collection: str,
        field: str,
        value: Any,
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        decoder: Callable[[Any], T],
    ) -> DocumentPage[T]: ...
    async def find_documents_by_field(
        self,
        tenant_id: str,
        collection: str,
        field: str,
        value: Any,
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        decoder: Optional[Callable[[Any], T]] = None,
    ) -> DocumentPage[Any]:
        """Find documents whose `field` equals the JSON scalar `value` (`FindByField`).

        `value` must encode to a JSON scalar - the server rejects an object or array
        with `INVALID_ARGUMENT`. `total_count` on the returned page is an index count
        over matching entries: not free (unlike `list_documents`), and not as expensive
        as `query_documents`.
        """
        response = await _call(
            "FindByField",
            self._documents.FindByField(
                pb.FindByFieldRequest(
                    tenant_id=tenant_id,
                    collection=collection,
                    field=field,
                    value_json=_encode_json(value, "search value"),
                    page=_page_request(limit, cursor),
                )
            ),
        )
        items = [_decode_json(item, "search results") for item in response.json]
        if decoder is not None:
            items = [decoder(item) for item in items]
        return DocumentPage(
            items=items,
            next_cursor=_optional_cursor(response.page.next_cursor),
            total_count=response.total_count,
        )

    @overload
    async def list_documents(
        self,
        tenant_id: str,
        collection: str,
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> DocumentPage[Any]: ...
    @overload
    async def list_documents(
        self,
        tenant_id: str,
        collection: str,
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        decoder: Callable[[Any], T],
    ) -> DocumentPage[T]: ...
    async def list_documents(
        self,
        tenant_id: str,
        collection: str,
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        decoder: Optional[Callable[[Any], T]] = None,
    ) -> DocumentPage[Any]:
        """Return one page of every document in `collection` (`ListDoc`).

        `total_count` on the returned page is free: the server keeps a running
        per-collection counter updated on every write, so reading it costs nothing
        beyond the listing itself.
        """
        response = await _call(
            "ListDoc",
            self._documents.ListDoc(
                pb.ListDocRequest(
                    tenant_id=tenant_id, collection=collection, page=_page_request(limit, cursor)
                )
            ),
        )
        items = [_decode_json(item, "listed documents") for item in response.json]
        if decoder is not None:
            items = [decoder(item) for item in items]
        return DocumentPage(
            items=items,
            next_cursor=_optional_cursor(response.page.next_cursor),
            total_count=response.total_count,
        )

    @overload
    async def query_documents(
        self,
        tenant_id: str,
        collection: str,
        filters: Sequence[DocumentQueryFilter] = (),
        sort: Sequence[DocumentQuerySort] = (),
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> DocumentPage[Any]: ...
    @overload
    async def query_documents(
        self,
        tenant_id: str,
        collection: str,
        filters: Sequence[DocumentQueryFilter] = (),
        sort: Sequence[DocumentQuerySort] = (),
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        decoder: Callable[[Any], T],
    ) -> DocumentPage[T]: ...
    async def query_documents(
        self,
        tenant_id: str,
        collection: str,
        filters: Sequence[DocumentQueryFilter] = (),
        sort: Sequence[DocumentQuerySort] = (),
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        decoder: Optional[Callable[[Any], T]] = None,
    ) -> DocumentPage[Any]:
        """Run a paginated, multi-filter, multi-sort document query (`QueryDoc`).

        Filters are ANDed together; sort levels are applied in the order given.
        `total_count` on the returned page is expensive - the server only knows it
        after filtering the complete candidate set, so its cost scales with that set on
        every call. Never loop on this just to obtain a count.
        """
        proto_filters = [
            pb.QueryFilter(
                field=item.field,
                operator=_QUERY_OPERATOR_TO_PROTO[item.operator],
                values_json=[_encode_json(v, "query filter value") for v in item.values],
            )
            for item in filters
        ]
        proto_sort = [
            pb.QuerySort(field=item.field, direction=_SORT_DIRECTION_TO_PROTO[item.direction])
            for item in sort
        ]
        response = await _call(
            "QueryDoc",
            self._documents.QueryDoc(
                pb.QueryDocRequest(
                    tenant_id=tenant_id,
                    collection=collection,
                    filters=proto_filters,
                    sort=proto_sort,
                    page=_page_request(limit, cursor),
                )
            ),
        )
        items = [_decode_json(item, "queried documents") for item in response.json]
        if decoder is not None:
            items = [decoder(item) for item in items]
        return DocumentPage(
            items=items,
            next_cursor=_optional_cursor(response.page.next_cursor),
            total_count=response.total_count,
        )

    async def list_collections(
        self, tenant_id: str, *, limit: Optional[int] = None, cursor: Optional[str] = None
    ) -> Page[CollectionInfo]:
        """List the document collections holding at least one document, each with its
        document count.
        """
        response = await _call(
            "ListCollections",
            self._documents.ListCollections(
                pb.ListCollectionsRequest(tenant_id=tenant_id, page=_page_request(limit, cursor))
            ),
        )
        items = [
            CollectionInfo(collection=item.collection, count=item.count)
            for item in response.collections
        ]
        return Page(items=items, next_cursor=_optional_cursor(response.page.next_cursor))

"""Unit tests for `_DocumentMixin`, exercised against fake (no-network) service stubs."""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest

from _doubles import FakeUnaryCall
from rociadb_sdk._pb.upstream.v1 import upstream_pb2 as pb
from rociadb_sdk.document import _DocumentMixin
from rociadb_sdk.errors import RociaDbValidationError
from rociadb_sdk.types import (
    DocumentQueryFilter,
    DocumentQueryOperator,
    DocumentQuerySort,
    DocumentSortDirection,
)


class _Client(_DocumentMixin):
    def __init__(self, documents: Any = None, graph: Any = None) -> None:
        self._documents = documents
        self._graph = graph


# --- put_document / delete_document: default request_id -----------------------------


async def test_put_document_defaults_request_id_to_put_document_collection_uuid() -> None:
    put_doc = FakeUnaryCall().returns(None)
    client = _Client(documents=_documents_stub(put_doc=put_doc))

    await client.put_document("tenant-1", "widgets", "doc-1", {"a": 1})

    request = put_doc.requests[0]
    assert request.tenant_id == "tenant-1"
    assert request.collection == "widgets"
    assert request.id == "doc-1"
    assert json.loads(request.json) == {"a": 1}
    assert request.request_id.startswith("put_document:widgets:")
    # The uuid suffix must actually be a valid uuid4, not just a non-empty string.
    uuid.UUID(request.request_id.rsplit(":", 1)[-1])


async def test_put_document_uses_a_supplied_request_id_unchanged() -> None:
    put_doc = FakeUnaryCall().returns(None)
    client = _Client(documents=_documents_stub(put_doc=put_doc))

    await client.put_document("t1", "widgets", "doc-1", {}, request_id="my-request-id")

    assert put_doc.requests[0].request_id == "my-request-id"


async def test_delete_document_defaults_request_id_to_delete_document_collection_uuid() -> None:
    delete_doc = FakeUnaryCall().returns(None)
    client = _Client(documents=_documents_stub(delete_doc=delete_doc))

    await client.delete_document("t1", "widgets", "doc-1")

    assert delete_doc.requests[0].request_id.startswith("delete_document:widgets:")


# --- create_document -----------------------------------------------------------------


async def test_create_document_rejects_node_label_without_node_graph_before_any_rpc() -> None:
    put_doc = FakeUnaryCall().returns(None)
    client = _Client(documents=_documents_stub(put_doc=put_doc))

    with pytest.raises(RociaDbValidationError):
        await client.create_document("t1", "widgets", "doc-1", {}, node_label="product")

    assert put_doc.requests == []  # validated before any network call


async def test_create_document_rejects_node_graph_without_node_label_before_any_rpc() -> None:
    put_doc = FakeUnaryCall().returns(None)
    client = _Client(documents=_documents_stub(put_doc=put_doc))

    with pytest.raises(RociaDbValidationError):
        await client.create_document("t1", "widgets", "doc-1", {}, node_graph="catalog")

    assert put_doc.requests == []


async def test_create_document_without_node_options_only_writes_the_document() -> None:
    put_doc = FakeUnaryCall().returns(None)
    put_node = FakeUnaryCall().returns(None)
    client = _Client(documents=_documents_stub(put_doc=put_doc), graph=_graph_stub(put_node))

    await client.create_document("t1", "widgets", "doc-1", {"a": 1})

    assert len(put_doc.requests) == 1
    assert put_node.requests == []


async def test_create_document_with_both_node_options_writes_document_then_label_id_node() -> None:
    put_doc = FakeUnaryCall().returns(None)
    put_node = FakeUnaryCall().returns(None)
    client = _Client(documents=_documents_stub(put_doc=put_doc), graph=_graph_stub(put_node))

    await client.create_document(
        "t1", "widgets", "doc-1", {"a": 1}, node_label="product", node_graph="catalog"
    )

    assert len(put_doc.requests) == 1
    assert len(put_node.requests) == 1
    node_request = put_node.requests[0]
    assert node_request.graph == "catalog"
    assert node_request.node_id == "product:doc-1"
    assert json.loads(node_request.json) == {"collection": "widgets", "id": "doc-1"}


# --- get_document: decoding and the decoder overload ----------------------------------


async def test_get_document_returns_decoded_json_without_a_decoder() -> None:
    get_doc = FakeUnaryCall().returns(pb.GetDocResponse(json=b'{"a": 1}'))
    client = _Client(documents=_documents_stub(get_doc=get_doc))

    value = await client.get_document("t1", "widgets", "doc-1")

    assert value == {"a": 1}
    request = get_doc.requests[0]
    assert (request.tenant_id, request.collection, request.id) == ("t1", "widgets", "doc-1")


async def test_get_document_applies_the_supplied_decoder() -> None:
    get_doc = FakeUnaryCall().returns(pb.GetDocResponse(json=b'{"a": 1}'))
    client = _Client(documents=_documents_stub(get_doc=get_doc))

    value = await client.get_document("t1", "widgets", "doc-1", decoder=lambda v: v["a"])

    assert value == 1


# --- find_documents_by_field / list_documents / query_documents: paging + total_count --


async def test_find_documents_by_field_rejects_and_decodes_correctly() -> None:
    find = FakeUnaryCall().returns(
        pb.FindByFieldResponse(
            json=[b'{"a": 1}', b'{"a": 2}'],
            page=pb.PageResponse(next_cursor="cursor-2"),
            total_count=42,
        )
    )
    client = _Client(documents=_documents_stub(find_by_field=find))

    page = await client.find_documents_by_field("t1", "widgets", "sku", "abc", limit=2)

    assert [item["a"] for item in page.items] == [1, 2]
    assert page.next_cursor == "cursor-2"
    assert page.total_count == 42
    request = find.requests[0]
    assert request.field == "sku"
    assert json.loads(request.value_json) == "abc"
    assert request.page.limit == 2


async def test_list_documents_decodes_items_and_applies_decoder() -> None:
    list_doc = FakeUnaryCall().returns(
        pb.ListDocResponse(json=[b'{"a": 1}'], page=pb.PageResponse(next_cursor=""), total_count=1)
    )
    client = _Client(documents=_documents_stub(list_doc=list_doc))

    page = await client.list_documents("t1", "widgets", decoder=lambda v: v["a"])

    assert page.items == [1]
    assert page.next_cursor is None  # empty proto cursor maps to None
    assert page.total_count == 1


async def test_query_documents_translates_filters_and_sort_to_proto() -> None:
    query = FakeUnaryCall().returns(
        pb.QueryDocResponse(json=[], page=pb.PageResponse(next_cursor=""), total_count=0)
    )
    client = _Client(documents=_documents_stub(query_doc=query))

    await client.query_documents(
        "t1",
        "widgets",
        filters=[
            DocumentQueryFilter(field="sku", operator=DocumentQueryOperator.IN, values=[1, 2])
        ],
        sort=[DocumentQuerySort(field="price", direction=DocumentSortDirection.DESC)],
    )

    request = query.requests[0]
    assert len(request.filters) == 1
    assert request.filters[0].field == "sku"
    assert request.filters[0].operator == pb.QUERY_OPERATOR_IN
    assert [json.loads(v) for v in request.filters[0].values_json] == [1, 2]
    assert len(request.sort) == 1
    assert request.sort[0].field == "price"
    assert request.sort[0].direction == pb.SORT_DIRECTION_DESC


# --- list_collections ------------------------------------------------------------------


async def test_list_collections_maps_every_entry() -> None:
    list_collections = FakeUnaryCall().returns(
        pb.ListCollectionsResponse(
            collections=[
                pb.CollectionInfo(collection="widgets", count=3),
                pb.CollectionInfo(collection="gadgets", count=0),
            ],
            page=pb.PageResponse(next_cursor=""),
        )
    )
    client = _Client(documents=_documents_stub(list_collections=list_collections))

    page = await client.list_collections("t1")

    assert [(c.collection, c.count) for c in page.items] == [("widgets", 3), ("gadgets", 0)]
    assert page.next_cursor is None


# --- fake stub construction helpers -----------------------------------------------------


class _FakeDocumentsStub:
    def __init__(self) -> None:
        self.PutDoc: Any = None
        self.GetDoc: Any = None
        self.DeleteDoc: Any = None
        self.FindByField: Any = None
        self.ListDoc: Any = None
        self.QueryDoc: Any = None
        self.ListCollections: Any = None


class _FakeGraphStub:
    def __init__(self, put_node: Any = None) -> None:
        self.PutNode = put_node


def _documents_stub(
    put_doc: Any = None,
    get_doc: Any = None,
    delete_doc: Any = None,
    find_by_field: Any = None,
    list_doc: Any = None,
    query_doc: Any = None,
    list_collections: Any = None,
) -> _FakeDocumentsStub:
    stub = _FakeDocumentsStub()
    stub.PutDoc = put_doc
    stub.GetDoc = get_doc
    stub.DeleteDoc = delete_doc
    stub.FindByField = find_by_field
    stub.ListDoc = list_doc
    stub.QueryDoc = query_doc
    stub.ListCollections = list_collections
    return stub


def _graph_stub(put_node: Any = None) -> _FakeGraphStub:
    return _FakeGraphStub(put_node=put_node)

"""A caller-supplied request_id reaches the server unchanged, empty string included.

Only omitting `request_id` asks the SDK to generate one. An empty string is a key the
caller chose, so treating it as absent would hand the server a fresh generated key on a
retry that was meant to be deduplicated -- and would make the same call behave
differently here than in the Rust and TypeScript SDKs, which both forward it verbatim.
"""

from __future__ import annotations

from typing import Any

from _doubles import FakeUnaryCall
from rocia_db_sdk.document import _DocumentMixin
from rocia_db_sdk.file import _FileMixin
from rocia_db_sdk.graph import _GraphMixin
from rocia_db_sdk.types import EdgeInput, NodeInput


class _Client(_DocumentMixin, _GraphMixin, _FileMixin):
    def __init__(self, documents: Any = None, graph: Any = None, files: Any = None) -> None:
        self._documents = documents
        self._graph = graph
        self._files = files


def _stub(rpc: str, count: int = 1) -> Any:
    call = FakeUnaryCall().returns(*[None] * count)
    return type("Stub", (), {rpc: call})()


async def test_put_document_forwards_an_empty_request_id() -> None:
    stub = _stub("PutDoc")
    await _Client(documents=stub).put_document("t", "products", "sku-1", {"a": 1}, request_id="")
    assert stub.PutDoc.requests[0].request_id == ""


async def test_delete_document_forwards_an_empty_request_id() -> None:
    stub = _stub("DeleteDoc")
    await _Client(documents=stub).delete_document("t", "products", "sku-1", request_id="")
    assert stub.DeleteDoc.requests[0].request_id == ""


async def test_put_node_forwards_an_empty_request_id() -> None:
    stub = _stub("PutNode")
    await _Client(graph=stub).put_node("t", "g", "product:sku-1", {"a": 1}, request_id="")
    assert stub.PutNode.requests[0].request_id == ""


async def test_add_edge_forwards_an_empty_request_id() -> None:
    stub = _stub("AddEdge")
    await _Client(graph=stub).add_edge("t", "g", "e1", "a", "b", "belongs_to", {}, request_id="")
    assert stub.AddEdge.requests[0].request_id == ""


async def test_delete_edge_forwards_an_empty_request_id() -> None:
    stub = _stub("DeleteEdge")
    await _Client(graph=stub).delete_edge("t", "g", "e1", request_id="")
    assert stub.DeleteEdge.requests[0].request_id == ""


async def test_delete_file_forwards_an_empty_request_id() -> None:
    stub = _stub("Delete")
    await _Client(files=stub).delete_file("t", "assets", "manual.txt", request_id="")
    assert stub.Delete.requests[0].request_id == ""


async def test_batch_node_input_forwards_an_empty_request_id() -> None:
    stub = _stub("PutNode")
    await _Client(graph=stub).put_nodes(
        "t", "g", [NodeInput(node_id="product:sku-1", value={}, request_id="")]
    )
    assert stub.PutNode.requests[0].request_id == ""


async def test_batch_edge_input_forwards_an_empty_request_id() -> None:
    stub = _stub("AddEdge")
    edge = EdgeInput(
        edge_id="e1", from_id="a", to_id="b", label="belongs_to", value={}, request_id=""
    )
    await _Client(graph=stub).add_edges("t", "g", [edge])
    assert stub.AddEdge.requests[0].request_id == ""

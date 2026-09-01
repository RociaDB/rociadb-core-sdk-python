"""Unit tests for `_GraphMixin`, exercised against fake (no-network) service stubs."""

from __future__ import annotations

import json
import uuid
from typing import Any, List

from _doubles import FakeUnaryCall
from rocia_db_sdk._pb.upstream.v1 import upstream_pb2 as pb
from rocia_db_sdk.graph import _GraphMixin
from rocia_db_sdk.types import EdgeInput, NodeInput


class _FakeGraphStub:
    def __init__(self) -> None:
        self.PutNode: Any = None
        self.GetNode: Any = None
        self.AddEdge: Any = None
        self.DeleteEdge: Any = None
        self.NeighborsOut: Any = None
        self.NeighborsIn: Any = None
        self.ListGraphs: Any = None
        self.ListNodes: Any = None


class _Client(_GraphMixin):
    def __init__(self, graph: Any) -> None:
        self._graph = graph


# --- put_node ---------------------------------------------------------------------


async def test_put_node_defaults_request_id_to_put_node_uuid() -> None:
    put_node = FakeUnaryCall().returns(None)
    stub = _FakeGraphStub()
    stub.PutNode = put_node
    client = _Client(stub)

    await client.put_node("t1", "catalog", "product:1", {"a": 1})

    request = put_node.requests[0]
    assert request.tenant_id == "t1"
    assert request.graph == "catalog"
    assert request.node_id == "product:1"
    assert json.loads(request.json) == {"a": 1}
    assert request.request_id.startswith("put_node:")
    uuid.UUID(request.request_id.split(":", 1)[1])


async def test_put_node_uses_a_supplied_request_id_unchanged() -> None:
    put_node = FakeUnaryCall().returns(None)
    stub = _FakeGraphStub()
    stub.PutNode = put_node
    client = _Client(stub)

    await client.put_node("t1", "catalog", "product:1", {}, request_id="my-id")

    assert put_node.requests[0].request_id == "my-id"


async def test_get_node_decodes_and_applies_decoder() -> None:
    get_node = FakeUnaryCall().returns(pb.GetNodeResponse(json=b'{"a": 1}'))
    stub = _FakeGraphStub()
    stub.GetNode = get_node
    client = _Client(stub)

    assert await client.get_node("t1", "catalog", "product:1") == {"a": 1}
    assert await client.get_node("t1", "catalog", "product:1", decoder=lambda v: v["a"]) == 1


# --- put_nodes: batch upsert, order, and duplicate node_ids ------------------------


async def test_put_nodes_preserves_order_and_does_not_merge_duplicate_node_ids() -> None:
    put_node = FakeUnaryCall().returns(None)
    stub = _FakeGraphStub()
    stub.PutNode = put_node
    client = _Client(stub)

    nodes = [
        NodeInput(node_id="a", value={"v": 1}),
        NodeInput(node_id="a", value={"v": 2}),  # duplicate node_id, distinct call
        NodeInput(node_id="b", value={"v": 3}, request_id="explicit-id"),
    ]
    await client.put_nodes("t1", "catalog", nodes)

    assert len(put_node.requests) == 3
    assert [r.node_id for r in put_node.requests] == ["a", "a", "b"]
    assert [json.loads(r.json)["v"] for r in put_node.requests] == [1, 2, 3]
    assert put_node.requests[2].request_id == "explicit-id"


async def test_put_nodes_on_an_empty_iterable_makes_no_calls() -> None:
    put_node = FakeUnaryCall().returns(None)
    stub = _FakeGraphStub()
    stub.PutNode = put_node
    client = _Client(stub)

    await client.put_nodes("t1", "catalog", [])

    assert put_node.requests == []


# --- add_edge: the `from`/`to` reserved-word field mapping --------------------------


async def test_add_edge_maps_from_id_to_id_onto_the_wire_from_to_fields() -> None:
    add_edge = FakeUnaryCall().returns(None)
    stub = _FakeGraphStub()
    stub.AddEdge = add_edge
    client = _Client(stub)

    await client.add_edge("t1", "catalog", "edge-1", "node-a", "node-b", "LINKS", {"w": 1})

    request = add_edge.requests[0]
    assert getattr(request, "from") == "node-a"
    assert request.to == "node-b"
    assert request.label == "LINKS"
    assert request.edge_id == "edge-1"
    # No prefix: a bare uuid4 string.
    uuid.UUID(request.request_id)


async def test_add_edge_uses_a_supplied_request_id_unchanged() -> None:
    add_edge = FakeUnaryCall().returns(None)
    stub = _FakeGraphStub()
    stub.AddEdge = add_edge
    client = _Client(stub)

    await client.add_edge("t1", "g", "e1", "a", "b", "L", {}, request_id="my-id")

    assert add_edge.requests[0].request_id == "my-id"


async def test_add_edges_preserves_order() -> None:
    add_edge = FakeUnaryCall().returns(None)
    stub = _FakeGraphStub()
    stub.AddEdge = add_edge
    client = _Client(stub)

    edges = [
        EdgeInput(edge_id="e1", from_id="a", to_id="b", label="L", value={}),
        EdgeInput(edge_id="e2", from_id="b", to_id="c", label="L", value={}),
    ]
    await client.add_edges("t1", "g", edges)

    assert [r.edge_id for r in add_edge.requests] == ["e1", "e2"]


# --- delete_edge ---------------------------------------------------------------------


async def test_delete_edge_defaults_request_id_to_delete_edge_uuid() -> None:
    delete_edge = FakeUnaryCall().returns(None)
    stub = _FakeGraphStub()
    stub.DeleteEdge = delete_edge
    client = _Client(stub)

    await client.delete_edge("t1", "g", "edge-1")

    assert delete_edge.requests[0].request_id.startswith("delete_edge:")


# --- neighbors_out / neighbors_in: `from`/`to` mapping and page decoding ------------


async def test_neighbors_out_maps_from_id_and_decodes_the_page() -> None:
    neighbors_out = FakeUnaryCall().returns(
        pb.NeighborsOutResponse(
            neighbors=[pb.Neighbor(node_id="b", edge_id="e1")],
            page=pb.PageResponse(next_cursor="cursor-2"),
        )
    )
    stub = _FakeGraphStub()
    stub.NeighborsOut = neighbors_out
    client = _Client(stub)

    page = await client.neighbors_out("t1", "g", "a", "LINKS", limit=10)

    assert getattr(neighbors_out.requests[0], "from") == "a"
    assert [(n.node_id, n.edge_id) for n in page.items] == [("b", "e1")]
    assert page.next_cursor == "cursor-2"


async def test_neighbors_in_maps_to_id_and_decodes_the_page() -> None:
    neighbors_in = FakeUnaryCall().returns(
        pb.NeighborsInResponse(
            neighbors=[pb.Neighbor(node_id="a", edge_id="e1")],
            page=pb.PageResponse(next_cursor=""),
        )
    )
    stub = _FakeGraphStub()
    stub.NeighborsIn = neighbors_in
    client = _Client(stub)

    page = await client.neighbors_in("t1", "g", "b", "LINKS")

    assert neighbors_in.requests[0].to == "b"
    assert page.next_cursor is None


async def test_list_graphs_and_list_nodes_decode_plain_string_pages() -> None:
    list_graphs = FakeUnaryCall().returns(
        pb.ListGraphsResponse(graphs=["catalog"], page=pb.PageResponse(next_cursor=""))
    )
    list_nodes = FakeUnaryCall().returns(
        pb.ListNodesResponse(node_ids=["a", "b"], page=pb.PageResponse(next_cursor=""))
    )
    stub = _FakeGraphStub()
    stub.ListGraphs = list_graphs
    stub.ListNodes = list_nodes
    client = _Client(stub)

    assert (await client.list_graphs("t1")).items == ["catalog"]
    assert (await client.list_nodes("t1", "catalog")).items == ["a", "b"]


# --- get_outgoing_neighbor_nodes / get_incoming_neighbor_nodes: page-following ------


class _ScriptedNeighborsOut:
    """A `NeighborsOut` fake that hands back a scripted sequence of pages, including a
    short, empty page mid-listing that still carries a fresh cursor - the SDK must keep
    following it rather than stopping on the first short page.
    """

    def __init__(self, pages: List[pb.NeighborsOutResponse]) -> None:
        self._pages = pages
        self.requests: List[Any] = []

    async def __call__(self, request: Any) -> pb.NeighborsOutResponse:
        self.requests.append(request)
        return self._pages[len(self.requests) - 1]


async def test_get_outgoing_neighbor_nodes_follows_a_short_empty_page_with_a_fresh_cursor() -> None:
    pages = [
        pb.NeighborsOutResponse(
            neighbors=[pb.Neighbor(node_id="b", edge_id="e1")],
            page=pb.PageResponse(next_cursor="cursor-2"),
        ),
        # A short/empty page mid-listing, but still carrying a fresh cursor: the loop
        # must not treat this as the end.
        pb.NeighborsOutResponse(neighbors=[], page=pb.PageResponse(next_cursor="cursor-3")),
        pb.NeighborsOutResponse(
            neighbors=[pb.Neighbor(node_id="c", edge_id="e2")],
            page=pb.PageResponse(next_cursor=""),
        ),
    ]
    neighbors_out = _ScriptedNeighborsOut(pages)
    get_node = FakeUnaryCall().returns(pb.GetNodeResponse(json=b'{"name": "x"}'))
    stub = _FakeGraphStub()
    stub.NeighborsOut = neighbors_out
    stub.GetNode = get_node
    client = _Client(stub)

    result = await client.get_outgoing_neighbor_nodes("t1", "g", "a", "LINKS")

    assert len(neighbors_out.requests) == 3  # kept following past the empty page
    assert sorted(n.node_id for n in result) == ["b", "c"]
    assert all(n.value == {"name": "x"} for n in result)


async def test_get_outgoing_neighbor_nodes_stops_on_absent_cursor_without_extra_calls() -> None:
    pages = [pb.NeighborsOutResponse(neighbors=[], page=pb.PageResponse(next_cursor=""))]
    neighbors_out = _ScriptedNeighborsOut(pages)
    stub = _FakeGraphStub()
    stub.NeighborsOut = neighbors_out
    client = _Client(stub)

    result = await client.get_outgoing_neighbor_nodes("t1", "g", "a", "LINKS")

    assert result == []
    assert len(neighbors_out.requests) == 1


async def test_get_outgoing_neighbor_nodes_applies_the_supplied_decoder() -> None:
    pages = [
        pb.NeighborsOutResponse(
            neighbors=[pb.Neighbor(node_id="b", edge_id="e1")],
            page=pb.PageResponse(next_cursor=""),
        )
    ]
    neighbors_out = _ScriptedNeighborsOut(pages)
    get_node = FakeUnaryCall().returns(pb.GetNodeResponse(json=b'{"name": "x"}'))
    stub = _FakeGraphStub()
    stub.NeighborsOut = neighbors_out
    stub.GetNode = get_node
    client = _Client(stub)

    result = await client.get_outgoing_neighbor_nodes(
        "t1", "g", "a", "LINKS", decoder=lambda v: v["name"]
    )

    assert [n.value for n in result] == ["x"]


class _ScriptedNeighborsIn:
    def __init__(self, pages: List[pb.NeighborsInResponse]) -> None:
        self._pages = pages
        self.requests: List[Any] = []

    async def __call__(self, request: Any) -> pb.NeighborsInResponse:
        self.requests.append(request)
        return self._pages[len(self.requests) - 1]


async def test_get_incoming_neighbor_nodes_follows_pages_and_hydrates() -> None:
    pages = [
        pb.NeighborsInResponse(
            neighbors=[pb.Neighbor(node_id="x", edge_id="e9")],
            page=pb.PageResponse(next_cursor=""),
        )
    ]
    neighbors_in = _ScriptedNeighborsIn(pages)
    get_node = FakeUnaryCall().returns(pb.GetNodeResponse(json=b'{"n": 1}'))
    stub = _FakeGraphStub()
    stub.NeighborsIn = neighbors_in
    stub.GetNode = get_node
    client = _Client(stub)

    result = await client.get_incoming_neighbor_nodes("t1", "g", "z", "LINKS")

    assert len(result) == 1
    assert result[0].node_id == "x"
    assert result[0].edge_id == "e9"
    assert result[0].value == {"n": 1}


async def test_neighbor_hydration_uses_the_fixed_internal_page_size_of_50() -> None:
    pages = [pb.NeighborsOutResponse(neighbors=[], page=pb.PageResponse(next_cursor=""))]
    neighbors_out = _ScriptedNeighborsOut(pages)
    stub = _FakeGraphStub()
    stub.NeighborsOut = neighbors_out
    client = _Client(stub)

    await client.get_outgoing_neighbor_nodes("t1", "g", "a", "LINKS")

    assert neighbors_out.requests[0].page.limit == 50

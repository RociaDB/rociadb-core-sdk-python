"""GraphService RPC methods: node/edge CRUD, neighbor listing, and neighbor hydration."""

from __future__ import annotations

from typing import Any, Callable, Iterable, List, Literal, Optional, TypeVar, overload
from uuid import uuid4

from rociadb_sdk._pagination import _next_pagination_cursor, _optional_cursor, _page_request
from rociadb_sdk._pb.upstream.v1 import upstream_pb2 as pb
from rociadb_sdk._pb.upstream.v1 import upstream_pb2_grpc as rpc
from rociadb_sdk.errors import _call, _decode_json, _encode_json
from rociadb_sdk.types import EdgeInput, Neighbor, NeighborNode, NodeInput, Page

T = TypeVar("T")

#: Fixed page size used internally while following neighbor pages for hydration,
#: independent of the caller's own pagination choices elsewhere.
_NEIGHBOR_HYDRATION_PAGE_SIZE = 50


def _neighbor_page(response: Any) -> Page[Neighbor]:
    items = [Neighbor(node_id=item.node_id, edge_id=item.edge_id) for item in response.neighbors]
    return Page(items=items, next_cursor=_optional_cursor(response.page.next_cursor))


class _GraphMixin:
    _graph: rpc.GraphServiceStub

    async def put_node(
        self,
        tenant_id: str,
        graph: str,
        node_id: str,
        value: Any,
        *,
        request_id: Optional[str] = None,
    ) -> None:
        """Create or replace one graph node using its complete node id (e.g. "product:42").

        `request_id` defaults to ``f"put_node:{uuid4()}"`` when omitted.
        """
        await _call(
            "PutNode",
            self._graph.PutNode(
                pb.PutNodeRequest(
                    tenant_id=tenant_id,
                    graph=graph,
                    node_id=node_id,
                    json=_encode_json(value, "node json"),
                    request_id=request_id if request_id is not None else f"put_node:{uuid4()}",
                )
            ),
        )

    async def put_nodes(self, tenant_id: str, graph: str, nodes: Iterable[NodeInput]) -> None:
        """Upsert a batch of nodes with at most `CONCURRENT_REQUESTS` calls in flight.

        Duplicate `node_id`s are not merged - each `NodeInput` becomes its own `PutNode`
        call. Not atomic: stops, and cancels the other in-flight calls, on the first
        failure. Every item carries its own idempotency key (defaulted the same way as
        `put_node` when omitted), so replaying the same batch after a failure is safe.
        """
        from rociadb_sdk.client import CONCURRENT_REQUESTS, _run_bounded

        async def worker(node: NodeInput) -> None:
            await self.put_node(
                tenant_id, graph, node.node_id, node.value, request_id=node.request_id
            )

        await _run_bounded(list(nodes), CONCURRENT_REQUESTS, worker)

    @overload
    async def get_node(self, tenant_id: str, graph: str, node_id: str) -> Any: ...
    @overload
    async def get_node(
        self, tenant_id: str, graph: str, node_id: str, *, decoder: Callable[[Any], T]
    ) -> T: ...
    async def get_node(
        self,
        tenant_id: str,
        graph: str,
        node_id: str,
        *,
        decoder: Optional[Callable[[Any], T]] = None,
    ) -> Any:
        """Fetch and JSON-decode one graph node."""
        response = await _call(
            "GetNode",
            self._graph.GetNode(
                pb.GetNodeRequest(tenant_id=tenant_id, graph=graph, node_id=node_id)
            ),
        )
        value = _decode_json(response.json, "node json")
        return decoder(value) if decoder is not None else value

    async def add_edge(
        self,
        tenant_id: str,
        graph: str,
        edge_id: str,
        from_id: str,
        to_id: str,
        label: str,
        value: Any,
        *,
        request_id: Optional[str] = None,
    ) -> None:
        """Create or replace one directed edge and its JSON payload.

        The server returns `NOT_FOUND` if `from_id` or `to_id` is not an existing node
        in `graph`: create both endpoint nodes before adding an edge between them.
        `request_id` defaults to a bare ``str(uuid4())`` (no prefix) when omitted.
        """
        request = pb.AddEdgeRequest(
            tenant_id=tenant_id,
            graph=graph,
            edge_id=edge_id,
            to=to_id,
            label=label,
            json=_encode_json(value, "edge json"),
            request_id=request_id if request_id is not None else str(uuid4()),
            **{"from": from_id},
        )
        await _call("AddEdge", self._graph.AddEdge(request))

    async def add_edges(self, tenant_id: str, graph: str, edges: Iterable[EdgeInput]) -> None:
        """Upsert a batch of edges with the same concurrency, ordering, atomicity, and
        replay-safety contract as `put_nodes`.
        """
        from rociadb_sdk.client import CONCURRENT_REQUESTS, _run_bounded

        async def worker(edge: EdgeInput) -> None:
            await self.add_edge(
                tenant_id,
                graph,
                edge.edge_id,
                edge.from_id,
                edge.to_id,
                edge.label,
                edge.value,
                request_id=edge.request_id,
            )

        await _run_bounded(list(edges), CONCURRENT_REQUESTS, worker)

    async def delete_edge(
        self, tenant_id: str, graph: str, edge_id: str, *, request_id: Optional[str] = None
    ) -> None:
        """Delete one edge by id.

        Unlike `delete_document`, the server returns `NOT_FOUND` for an edge that does
        not exist rather than treating the delete as idempotent.
        """
        await _call(
            "DeleteEdge",
            self._graph.DeleteEdge(
                pb.DeleteEdgeRequest(
                    tenant_id=tenant_id,
                    graph=graph,
                    edge_id=edge_id,
                    request_id=request_id if request_id is not None else f"delete_edge:{uuid4()}",
                )
            ),
        )

    async def neighbors_out(
        self,
        tenant_id: str,
        graph: str,
        from_id: str,
        label: str,
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> Page[Neighbor]:
        """Return one page of outgoing neighbors of `from_id` along `label`."""
        request = pb.NeighborsOutRequest(
            tenant_id=tenant_id,
            graph=graph,
            label=label,
            page=_page_request(limit, cursor),
            **{"from": from_id},
        )
        response = await _call("NeighborsOut", self._graph.NeighborsOut(request))
        return _neighbor_page(response)

    async def neighbors_in(
        self,
        tenant_id: str,
        graph: str,
        to_id: str,
        label: str,
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> Page[Neighbor]:
        """Return one page of incoming neighbors of `to_id` along `label`."""
        response = await _call(
            "NeighborsIn",
            self._graph.NeighborsIn(
                pb.NeighborsInRequest(
                    tenant_id=tenant_id,
                    graph=graph,
                    to=to_id,
                    label=label,
                    page=_page_request(limit, cursor),
                )
            ),
        )
        return _neighbor_page(response)

    async def list_graphs(
        self, tenant_id: str, *, limit: Optional[int] = None, cursor: Optional[str] = None
    ) -> Page[str]:
        """List the graph names holding at least one node."""
        response = await _call(
            "ListGraphs",
            self._graph.ListGraphs(
                pb.ListGraphsRequest(tenant_id=tenant_id, page=_page_request(limit, cursor))
            ),
        )
        return Page(
            items=list(response.graphs), next_cursor=_optional_cursor(response.page.next_cursor)
        )

    async def list_nodes(
        self,
        tenant_id: str,
        graph: str,
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> Page[str]:
        """List the node ids stored in one graph."""
        response = await _call(
            "ListNodes",
            self._graph.ListNodes(
                pb.ListNodesRequest(
                    tenant_id=tenant_id, graph=graph, page=_page_request(limit, cursor)
                )
            ),
        )
        return Page(
            items=list(response.node_ids), next_cursor=_optional_cursor(response.page.next_cursor)
        )

    @overload
    async def get_outgoing_neighbor_nodes(
        self, tenant_id: str, graph: str, node_id: str, label: str
    ) -> List[NeighborNode[Any]]: ...
    @overload
    async def get_outgoing_neighbor_nodes(
        self,
        tenant_id: str,
        graph: str,
        node_id: str,
        label: str,
        *,
        decoder: Callable[[Any], T],
    ) -> List[NeighborNode[T]]: ...
    async def get_outgoing_neighbor_nodes(
        self,
        tenant_id: str,
        graph: str,
        node_id: str,
        label: str,
        *,
        decoder: Optional[Callable[[Any], T]] = None,
    ) -> List[NeighborNode[Any]]:
        """Follow every outgoing-neighbor page of `node_id` and hydrate each neighbor's
        decoded node payload, with bounded concurrency.
        """
        return await self._hydrate_neighbors("out", tenant_id, graph, node_id, label, decoder)

    @overload
    async def get_incoming_neighbor_nodes(
        self, tenant_id: str, graph: str, node_id: str, label: str
    ) -> List[NeighborNode[Any]]: ...
    @overload
    async def get_incoming_neighbor_nodes(
        self,
        tenant_id: str,
        graph: str,
        node_id: str,
        label: str,
        *,
        decoder: Callable[[Any], T],
    ) -> List[NeighborNode[T]]: ...
    async def get_incoming_neighbor_nodes(
        self,
        tenant_id: str,
        graph: str,
        node_id: str,
        label: str,
        *,
        decoder: Optional[Callable[[Any], T]] = None,
    ) -> List[NeighborNode[Any]]:
        """Follow every incoming-neighbor page of `node_id` and hydrate each neighbor's
        decoded node payload, with bounded concurrency.
        """
        return await self._hydrate_neighbors("in", tenant_id, graph, node_id, label, decoder)

    async def _hydrate_neighbors(
        self,
        direction: Literal["out", "in"],
        tenant_id: str,
        graph: str,
        node_id: str,
        label: str,
        decoder: Optional[Callable[[Any], T]],
    ) -> List[NeighborNode[Any]]:
        from rociadb_sdk.client import CONCURRENT_REQUESTS, _run_bounded

        neighbors: List[Neighbor] = []
        cursor: Optional[str] = None
        while True:
            if direction == "out":
                page = await self.neighbors_out(
                    tenant_id,
                    graph,
                    node_id,
                    label,
                    limit=_NEIGHBOR_HYDRATION_PAGE_SIZE,
                    cursor=cursor,
                )
            else:
                page = await self.neighbors_in(
                    tenant_id,
                    graph,
                    node_id,
                    label,
                    limit=_NEIGHBOR_HYDRATION_PAGE_SIZE,
                    cursor=cursor,
                )
            neighbors.extend(page.items)
            next_cursor = _next_pagination_cursor(cursor, page.next_cursor)
            if next_cursor is None:
                break
            cursor = next_cursor

        async def worker(neighbor: Neighbor) -> NeighborNode[Any]:
            value = await self.get_node(tenant_id, graph, neighbor.node_id)
            if decoder is not None:
                value = decoder(value)
            return NeighborNode(node_id=neighbor.node_id, edge_id=neighbor.edge_id, value=value)

        return await _run_bounded(neighbors, CONCURRENT_REQUESTS, worker)

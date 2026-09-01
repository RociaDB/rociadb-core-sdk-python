"""Unit tests for `_TenantMixin`, exercised against a fake (no-network) service stub."""

from __future__ import annotations

from typing import Any

from _doubles import FakeUnaryCall
from rociadb_sdk._pb.upstream.v1 import upstream_pb2 as pb
from rociadb_sdk.tenant import _TenantMixin


class _FakeTenantsStub:
    def __init__(self, list_tenants: Any) -> None:
        self.ListTenants = list_tenants


class _Client(_TenantMixin):
    def __init__(self, tenants: Any) -> None:
        self._tenants = tenants


async def test_list_tenants_decodes_ids_and_maps_the_cursor() -> None:
    list_tenants = FakeUnaryCall().returns(
        pb.ListTenantsResponse(tenant_ids=["t1", "t2"], page=pb.PageResponse(next_cursor="c2"))
    )
    client = _Client(_FakeTenantsStub(list_tenants))

    page = await client.list_tenants(limit=2)

    assert page.items == ["t1", "t2"]
    assert page.next_cursor == "c2"
    assert list_tenants.requests[0].page.limit == 2


async def test_list_tenants_is_not_scoped_to_a_tenant_id() -> None:
    # ListTenantsRequest carries only a page, never a tenant_id - the request must not
    # even offer a place to put one.
    assert not hasattr(pb.ListTenantsRequest(), "tenant_id")

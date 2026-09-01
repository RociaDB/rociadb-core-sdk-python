"""TenantService RPC methods."""

from __future__ import annotations

from typing import Optional

from rociadb_sdk._pagination import _optional_cursor, _page_request
from rociadb_sdk._pb.upstream.v1 import upstream_pb2 as pb
from rociadb_sdk._pb.upstream.v1 import upstream_pb2_grpc as rpc
from rociadb_sdk.errors import _call
from rociadb_sdk.types import Page


class _TenantMixin:
    _tenants: rpc.TenantServiceStub

    async def list_tenants(
        self, *, limit: Optional[int] = None, cursor: Optional[str] = None
    ) -> Page[str]:
        """List the tenant ids known to the deployment.

        Not scoped to a tenant itself - it enumerates the whole deployment - and may be
        restricted by a dedicated server-side authorization policy.
        """
        response = await _call(
            "ListTenants",
            self._tenants.ListTenants(pb.ListTenantsRequest(page=_page_request(limit, cursor))),
        )
        return Page(
            items=list(response.tenant_ids),
            next_cursor=_optional_cursor(response.page.next_cursor),
        )

"""The connected client: `RociaDbBuilder`, `RociaDbClient`, and the bounded-concurrency
batch runner shared by the graph service's batch writes and neighbor-hydration reads
(`put_nodes`, `add_edges`, `get_outgoing_neighbor_nodes`, `get_incoming_neighbor_nodes`).
"""

from __future__ import annotations

import asyncio
import os
import urllib.parse
from typing import Any, Awaitable, Callable, List, Optional, Sequence, Tuple, TypeVar

import grpc
import grpc.aio

from rociadb_sdk._pb.upstream.v1 import upstream_pb2_grpc as rpc
from rociadb_sdk.auth import (
    AuthStreamUnaryInterceptor,
    AuthUnaryStreamInterceptor,
    AuthUnaryUnaryInterceptor,
    TokenManager,
    TokenRefreshHandle,
)
from rociadb_sdk.document import _DocumentMixin
from rociadb_sdk.errors import RociaDbConnectionError, RociaDbValidationError
from rociadb_sdk.file import _FileMixin
from rociadb_sdk.graph import _GraphMixin
from rociadb_sdk.tenant import _TenantMixin

#: Max in-flight calls for `put_nodes`/`add_edges`/`get_*_neighbor_nodes`.
CONCURRENT_REQUESTS = 10

#: Applied by `RociaDbBuilder.build`/`RociaDbClient.connect` when no host is set.
DEFAULT_HOST = "http://127.0.0.1:50051"

#: Applied by `RociaDbBuilder.build`/`RociaDbClient.connect` when no connect timeout is set.
DEFAULT_CONNECT_TIMEOUT = 10.0

_AUTH_TOKEN_URL_ENV = "AUTH_TOKEN_URL"
_AUTH_CLIENT_ID_ENV = "AUTH_CLIENT_ID"
_AUTH_CLIENT_SECRET_ENV = "AUTH_CLIENT_SECRET"

_In = TypeVar("_In")
_Out = TypeVar("_Out")


async def _run_bounded(
    items: Sequence[_In],
    concurrency: int,
    worker: Callable[[_In], Awaitable[_Out]],
) -> List[_Out]:
    """Run `worker` over `items` with at most `concurrency` calls in flight at once.

    Not atomic: as soon as one call raises, every task still pending is cancelled and
    that first failure is re-raised. On success, the returned list is in `items` order
    regardless of completion order. Every write this drives carries its own idempotency
    key, so cancelling in-flight siblings only reduces wasted server-side work - it is
    always safe to replay the same batch afterward.

    Built on `asyncio.Semaphore` + `asyncio.gather` rather than `asyncio.TaskGroup`,
    which is unavailable on this package's Python 3.10 floor.
    """
    if not items:
        return []

    semaphore = asyncio.Semaphore(concurrency)

    async def guarded(item: _In) -> _Out:
        async with semaphore:
            return await worker(item)

    tasks = [asyncio.ensure_future(guarded(item)) for item in items]
    try:
        results = await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    return list(results)


def _resolve_endpoint(host: str) -> Tuple[str, bool]:
    """Parse `host` into a gRPC dial target and a TLS flag.

    `https` activates TLS, `http` does not; a missing port takes the scheme's default
    (443 or 80); a path other than the root is rejected. TLS terminates at a reverse
    proxy in front of the server, so `https://host` (or an explicit `:443`) is the
    normal production endpoint even though the server itself always speaks plaintext.
    """
    parsed = urllib.parse.urlsplit(host)
    if parsed.scheme not in ("http", "https"):
        raise RociaDbConnectionError(
            f"RociaDB host must start with http:// or https://, got {host!r}"
        )
    if not parsed.hostname:
        raise RociaDbConnectionError(f"RociaDB host is missing a hostname: {host!r}")
    if parsed.path not in ("", "/"):
        raise RociaDbConnectionError(
            f"RociaDB host must contain only a hostname and port, got path {parsed.path!r}"
        )
    use_tls = parsed.scheme == "https"
    port = parsed.port if parsed.port is not None else (443 if use_tls else 80)
    return f"{parsed.hostname}:{port}", use_tls


def _resolve_connect_timeout(explicit: Optional[float]) -> float:
    """Resolve the connect deadline `build()`/`connect()` applies, in seconds.

    `DEFAULT_CONNECT_TIMEOUT` when `explicit` is `None`; either way, rejects a
    non-positive value with `RociaDbValidationError` before any connection attempt.
    """
    timeout = DEFAULT_CONNECT_TIMEOUT if explicit is None else explicit
    if timeout <= 0:
        raise RociaDbValidationError("connect timeout must be greater than zero")
    return timeout


class RociaDbBuilder:
    """Fluent builder for a connected `RociaDbClient`.

    Defaults to host ``"http://127.0.0.1:50051"``, auth enabled with per-field fallback
    to the `AUTH_TOKEN_URL`/`AUTH_CLIENT_ID`/`AUTH_CLIENT_SECRET` environment variables,
    and a 10-second connect timeout.
    """

    def __init__(self) -> None:
        self._host = DEFAULT_HOST
        self._auth_token_url: Optional[str] = None
        self._auth_client_id: Optional[str] = None
        self._auth_client_secret: Optional[str] = None
        self._auth_disabled = False
        self._connect_timeout: Optional[float] = None

    def host(self, host: str) -> RociaDbBuilder:
        """Set the gRPC endpoint. The URL scheme selects insecure HTTP or TLS."""
        self._host = host
        return self

    def auth_client_credentials(
        self, token_url: str, client_id: str, client_secret: str
    ) -> RociaDbBuilder:
        """Configure OAuth2 client-credentials auth explicitly, overriding env vars."""
        self._auth_token_url = token_url
        self._auth_client_id = client_id
        self._auth_client_secret = client_secret
        self._auth_disabled = False
        return self

    def disable_auth(self) -> RociaDbBuilder:
        """Disable outgoing auth metadata entirely."""
        self._auth_disabled = True
        return self

    def connect_timeout(self, timeout: float) -> RociaDbBuilder:
        """Set the connect deadline, in seconds.

        Validated immediately: raises `RociaDbValidationError` if `timeout` is not
        greater than zero.
        """
        if timeout <= 0:
            raise RociaDbValidationError("connect timeout must be greater than zero")
        self._connect_timeout = timeout
        return self

    async def build(self) -> RociaDbClient:
        """Fetch the first token (if auth enabled), connect every service, and return a
        ready client.
        """
        return await RociaDbClient.connect(
            self._host,
            auth_token_url=self._auth_token_url,
            auth_client_id=self._auth_client_id,
            auth_client_secret=self._auth_client_secret,
            disable_auth=self._auth_disabled,
            connect_timeout=_resolve_connect_timeout(self._connect_timeout),
        )


class RociaDbClient(_DocumentMixin, _GraphMixin, _FileMixin, _TenantMixin):
    """Async gRPC client for RociaDB's document, graph, file, and tenant services.

    Build one with `RociaDbBuilder` or `RociaDbClient.connect`. A single instance is
    safe to share and call concurrently from many asyncio tasks with no extra
    synchronization or cloning step. Call `close()` (or use it as an `async with`
    context manager) during shutdown; the client must not be reused afterward.
    """

    def __init__(
        self,
        channel: grpc.aio.Channel,
        token_manager: Optional[TokenManager],
        token_refresh_handle: Optional[TokenRefreshHandle],
    ) -> None:
        self._channel = channel
        self._token_manager = token_manager
        self._token_refresh_handle = token_refresh_handle
        # protoc emits these Stub classes without a companion .pyi, so unlike the
        # message types in upstream_pb2 their __init__ has no stub-provided signature.
        self._documents = rpc.DocumentServiceStub(channel)  # type: ignore[no-untyped-call]
        self._graph = rpc.GraphServiceStub(channel)  # type: ignore[no-untyped-call]
        self._files = rpc.FileServiceStub(channel)  # type: ignore[no-untyped-call]
        self._tenants = rpc.TenantServiceStub(channel)  # type: ignore[no-untyped-call]
        self._closed = False

    @classmethod
    async def connect(
        cls,
        host: str = DEFAULT_HOST,
        *,
        auth_token_url: Optional[str] = None,
        auth_client_id: Optional[str] = None,
        auth_client_secret: Optional[str] = None,
        disable_auth: bool = False,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    ) -> RociaDbClient:
        """Connect without the fluent builder, covering the same options in one call.

        Auth is enabled by default. Each of `auth_token_url`/`auth_client_id`/
        `auth_client_secret` that is omitted falls back independently to the
        `AUTH_TOKEN_URL`/`AUTH_CLIENT_ID`/`AUTH_CLIENT_SECRET` environment variables;
        this raises `RociaDbConnectionError` if a field is missing from both sources.
        Raises `RociaDbConnectionError` if the channel is not ready within
        `connect_timeout` seconds, and `RociaDbAuthError` if the first token fetch
        fails.
        """
        target, use_tls = _resolve_endpoint(host)
        timeout = _resolve_connect_timeout(connect_timeout)

        token_manager: Optional[TokenManager] = None
        if not disable_auth:
            token_url = auth_token_url or os.environ.get(_AUTH_TOKEN_URL_ENV)
            client_id = auth_client_id or os.environ.get(_AUTH_CLIENT_ID_ENV)
            client_secret = auth_client_secret or os.environ.get(_AUTH_CLIENT_SECRET_ENV)
            if not token_url:
                raise RociaDbConnectionError(
                    "missing auth token url (set AUTH_TOKEN_URL or pass auth_token_url)"
                )
            if not client_id:
                raise RociaDbConnectionError(
                    "missing auth client id (set AUTH_CLIENT_ID or pass auth_client_id)"
                )
            if not client_secret:
                raise RociaDbConnectionError(
                    "missing auth client secret (set AUTH_CLIENT_SECRET or pass auth_client_secret)"
                )
            token_manager = TokenManager(token_url, client_id, client_secret)
            await token_manager.refresh_now()

        interceptors: List[Any] = []
        if token_manager is not None:
            interceptors = [
                AuthUnaryUnaryInterceptor(token_manager),
                AuthUnaryStreamInterceptor(token_manager),
                AuthStreamUnaryInterceptor(token_manager),
            ]

        channel = (
            grpc.aio.secure_channel(
                target, grpc.ssl_channel_credentials(), interceptors=interceptors
            )
            if use_tls
            else grpc.aio.insecure_channel(target, interceptors=interceptors)
        )
        try:
            await asyncio.wait_for(channel.channel_ready(), timeout=timeout)
        except (asyncio.TimeoutError, grpc.aio.AioRpcError, OSError) as exc:
            await channel.close()
            raise RociaDbConnectionError(f"failed to connect to {host}: {exc}") from exc

        token_refresh_handle: Optional[TokenRefreshHandle] = None
        if token_manager is not None:
            token_refresh_handle = token_manager.spawn_refresh(token_manager.refresh_interval())

        return cls(channel, token_manager, token_refresh_handle)

    async def close(self) -> None:
        """Close the underlying gRPC channel and stop the background token-refresh task.

        The client must not be reused afterward. Safe to call more than once.
        """
        if self._closed:
            return
        self._closed = True
        if self._token_refresh_handle is not None:
            await self._token_refresh_handle.aclose()
        await self._channel.close()

    async def __aenter__(self) -> RociaDbClient:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def refresh_auth_token(self) -> None:
        """Force an immediate, blocking refresh of the cached auth token.

        No-op if the client was built with auth disabled.
        """
        if self._token_manager is not None:
            await self._token_manager.refresh_now()

    def invalidate_auth_token(self) -> None:
        """Mark the cached auth token stale without waiting for the refresh round trip.

        No-op if the client was built with auth disabled.
        """
        if self._token_manager is not None:
            self._token_manager.request_refresh()

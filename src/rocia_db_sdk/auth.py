"""OAuth2 client-credentials token handling: fetch, cache, and background refresh.

Also hosts the `grpc.aio` interceptors that attach the cached bearer header to
outgoing calls - one per RPC shape, since `grpc.aio` intercepts unary-unary (every
Document/Graph/Tenant/File call except `Upload`/`Download`), unary-stream (`Download`),
and stream-unary (`Upload`) calls through three distinct interfaces.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, List, Optional, Tuple

import grpc
import grpc.aio

from rocia_db_sdk.errors import RociaDbAuthError, RociaDbError

_logger = logging.getLogger(__name__)

#: Floor applied to the computed background refresh interval, in case the IdP ever
#: advertises a very short (or zero) token lifetime.
MIN_REFRESH_INTERVAL = 5.0

#: `TokenManager`'s assumed token lifetime, in seconds, before any token has been
#: fetched - only `refresh_interval()` can observe it, and only before the first
#: successful `refresh_now()`. Matches the IdP's actual fixed lifetime.
_DEFAULT_TOKEN_LIFETIME = 600


@dataclass(frozen=True)
class TokenResponse:
    """Fields of a successful OAuth2 client-credentials token response."""

    access_token: str
    token_type: str
    expires_in: int


def _fetch_token_sync(token_url: str, client_id: str, client_secret: str) -> TokenResponse:
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
    ).encode("ascii")
    request = urllib.request.Request(
        token_url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise RociaDbAuthError(f"OAuth token request to {token_url} failed: {exc}") from exc
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RociaDbAuthError("OAuth token response is not valid JSON") from exc

    try:
        return TokenResponse(
            access_token=str(payload["access_token"]),
            token_type=str(payload["token_type"]),
            expires_in=int(payload["expires_in"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RociaDbAuthError("OAuth token response is missing required fields") from exc


async def fetch_token(token_url: str, client_id: str, client_secret: str) -> TokenResponse:
    """Fetch one OAuth2 client-credentials token from the IdP, without caching.

    Runs the blocking HTTP POST in the default executor so the event loop is never
    blocked waiting on the IdP.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _fetch_token_sync, token_url, client_id, client_secret)


class TokenManager:
    """Fetches, caches, and background-refreshes an OAuth2 client-credentials token.

    Concurrent `refresh_now()` calls (including the implicit one inside
    `get_authorization_header()`) are deduplicated into a single in-flight fetch. A
    failed refresh always raises without touching the cached header: `_authorization`
    is only ever overwritten once a fetch succeeds, so a still-valid cached token is
    never discarded because a later refresh attempt failed.
    """

    def __init__(self, token_url: str, client_id: str, client_secret: str) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._authorization: Optional[str] = None
        self._expires_in: int = _DEFAULT_TOKEN_LIFETIME
        self._inflight: Optional[asyncio.Task[None]] = None
        self._refresh_requested = asyncio.Event()

    def refresh_interval(self) -> float:
        """Safe background-refresh interval derived from the last reported token
        lifetime: ``max(expires_in * 2 // 3, MIN_REFRESH_INTERVAL)`` seconds, leaving
        margin so the token never actually expires between two refreshes. With the
        IdP's fixed 600-second lifetime this is 400 seconds.
        """
        return max(self._expires_in * 2 // 3, MIN_REFRESH_INTERVAL)

    async def get_authorization_header(self) -> str:
        """Return the cached ``"<token_type> <access_token>"`` header.

        Fetches a token first if none is cached yet.
        """
        if self._authorization is None:
            await self.refresh_now()
        assert self._authorization is not None
        return self._authorization

    async def refresh_now(self) -> None:
        """Force an immediate, blocking token refresh.

        Concurrent callers await the same in-flight fetch rather than each starting
        their own. Raises `RociaDbAuthError` on failure without discarding whatever
        header is currently cached.
        """
        if self._inflight is None or self._inflight.done():
            self._inflight = asyncio.ensure_future(self._do_refresh())
        await self._inflight

    async def _do_refresh(self) -> None:
        token = await fetch_token(self._token_url, self._client_id, self._client_secret)
        self._authorization = f"{token.token_type} {token.access_token}"
        self._expires_in = token.expires_in

    def request_refresh(self) -> None:
        """Wake the task started by `spawn_refresh` without waiting for the round trip.

        A harmless no-op when no background task is running.
        """
        self._refresh_requested.set()

    def spawn_refresh(self, interval: float) -> TokenRefreshHandle:
        """Start a background task that refreshes the token every `interval` seconds,
        or immediately when woken by `request_refresh`, until the returned handle is
        closed. A failed background refresh is logged and retried on the next tick
        rather than raised anywhere - there is no caller left in the loop to raise to.
        """
        task = asyncio.ensure_future(self._refresh_loop(interval))
        return TokenRefreshHandle(task)

    async def _refresh_loop(self, interval: float) -> None:
        while True:
            try:
                await asyncio.wait_for(self._refresh_requested.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
            else:
                self._refresh_requested.clear()
            try:
                await self.refresh_now()
            except RociaDbError as exc:
                _logger.warning("RociaDB auth token refresh failed: %s", exc)


class TokenRefreshHandle:
    """Handle for the background task started by `TokenManager.spawn_refresh`.

    Close it when the owning client shuts down - the task does not stop on its own.
    """

    def __init__(self, task: asyncio.Task[None]) -> None:
        self._task = task

    async def aclose(self) -> None:
        """Cancel the background refresh task and wait for it to actually stop."""
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


async def _with_auth_metadata(
    client_call_details: grpc.aio.ClientCallDetails, token_manager: TokenManager
) -> grpc.aio.ClientCallDetails:
    """Return `client_call_details` with an `authorization` entry appended to its
    metadata, fetching/reusing the cached bearer header from `token_manager`.
    """
    header = await token_manager.get_authorization_header()
    metadata: List[Tuple[str, str]] = list(client_call_details.metadata or [])
    metadata.append(("authorization", header))
    return grpc.aio.ClientCallDetails(
        method=client_call_details.method,
        timeout=client_call_details.timeout,
        metadata=grpc.aio.Metadata(*metadata),
        credentials=client_call_details.credentials,
        wait_for_ready=client_call_details.wait_for_ready,
    )


class AuthUnaryUnaryInterceptor(grpc.aio.UnaryUnaryClientInterceptor):  # type: ignore[misc]
    """Attaches the cached bearer token to every unary-unary call, across all services."""

    def __init__(self, token_manager: TokenManager) -> None:
        self._token_manager = token_manager

    async def intercept_unary_unary(
        self,
        continuation: Callable[[grpc.aio.ClientCallDetails, Any], Awaitable[Any]],
        client_call_details: grpc.aio.ClientCallDetails,
        request: Any,
    ) -> Any:
        details = await _with_auth_metadata(client_call_details, self._token_manager)
        return await continuation(details, request)


class AuthUnaryStreamInterceptor(grpc.aio.UnaryStreamClientInterceptor):  # type: ignore[misc]
    """Attaches the cached bearer token to the unary-stream `Download` call."""

    def __init__(self, token_manager: TokenManager) -> None:
        self._token_manager = token_manager

    async def intercept_unary_stream(
        self,
        continuation: Callable[[grpc.aio.ClientCallDetails, Any], Any],
        client_call_details: grpc.aio.ClientCallDetails,
        request: Any,
    ) -> Any:
        details = await _with_auth_metadata(client_call_details, self._token_manager)
        return await continuation(details, request)


class AuthStreamUnaryInterceptor(grpc.aio.StreamUnaryClientInterceptor):  # type: ignore[misc]
    """Attaches the cached bearer token to the stream-unary `Upload` call."""

    def __init__(self, token_manager: TokenManager) -> None:
        self._token_manager = token_manager

    async def intercept_stream_unary(
        self,
        continuation: Callable[[grpc.aio.ClientCallDetails, Any], Awaitable[Any]],
        client_call_details: grpc.aio.ClientCallDetails,
        request_iterator: Any,
    ) -> Any:
        details = await _with_auth_metadata(client_call_details, self._token_manager)
        return await continuation(details, request_iterator)

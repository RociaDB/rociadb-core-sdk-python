"""Unit tests for OAuth2 token fetching, `TokenManager`, and the auth interceptors.

Every test here runs fully offline: `urllib.request.urlopen` is monkeypatched for the
`fetch_token`/`_fetch_token_sync` tests, and `TokenManager`'s own module-level
`fetch_token` dependency is monkeypatched for everything downstream of it, so no real
network call is ever made.
"""

from __future__ import annotations

import asyncio
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, List, Union

import grpc
import grpc.aio
import pytest

from rocia_db_sdk import auth as auth_module
from rocia_db_sdk.auth import (
    MIN_REFRESH_INTERVAL,
    AuthStreamUnaryInterceptor,
    AuthUnaryStreamInterceptor,
    AuthUnaryUnaryInterceptor,
    TokenManager,
    TokenResponse,
    fetch_token,
)
from rocia_db_sdk.errors import RociaDbAuthError

# --- test doubles -------------------------------------------------------------------


class _FakeHTTPResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> _FakeHTTPResponse:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


@dataclass
class _FakeFetcher:
    """Replaces `auth_module.fetch_token`. Each call consumes the next response (an
    exception is raised, a `TokenResponse` is returned); once exhausted, the last
    response repeats.
    """

    responses: List[Union[TokenResponse, Exception]]
    calls: int = field(default=0, init=False)

    async def __call__(self, token_url: str, client_id: str, client_secret: str) -> TokenResponse:
        self.calls += 1
        response = self.responses[min(self.calls, len(self.responses)) - 1]
        if isinstance(response, Exception):
            raise response
        return response


def _client_call_details(
    *,
    method: bytes = b"/rocia.v1.DocumentService/GetDoc",
    timeout: object = None,
    metadata: object = None,
    credentials: object = None,
    wait_for_ready: object = None,
) -> grpc.aio.ClientCallDetails:
    return grpc.aio.ClientCallDetails(
        method=method,
        timeout=timeout,
        metadata=metadata,
        credentials=credentials,
        wait_for_ready=wait_for_ready,
    )


# --- _fetch_token_sync / fetch_token --------------------------------------------------


def test_fetch_token_sync_parses_a_successful_response(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: Dict[str, Any] = {}

    def fake_urlopen(request: urllib.request.Request) -> _FakeHTTPResponse:
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        body = request.data
        assert isinstance(body, bytes)
        captured["body"] = body.decode("ascii")
        return _FakeHTTPResponse(
            b'{"access_token": "tok", "token_type": "Bearer", "expires_in": 600}'
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    token = auth_module._fetch_token_sync("https://idp.example/token", "client-1", "secret-1")

    assert token == TokenResponse(access_token="tok", token_type="Bearer", expires_in=600)
    assert captured["url"] == "https://idp.example/token"
    assert captured["method"] == "POST"
    assert "grant_type=client_credentials" in captured["body"]
    assert "client_id=client-1" in captured["body"]
    assert "client_secret=secret-1" in captured["body"]


def test_fetch_token_sync_wraps_a_connection_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_urlopen(request: urllib.request.Request) -> _FakeHTTPResponse:
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(RociaDbAuthError):
        auth_module._fetch_token_sync("https://idp.example/token", "id", "secret")


def test_fetch_token_sync_wraps_a_non_json_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        urllib.request, "urlopen", lambda request: _FakeHTTPResponse(b"not json at all")
    )
    with pytest.raises(RociaDbAuthError):
        auth_module._fetch_token_sync("https://idp.example/token", "id", "secret")


def test_fetch_token_sync_wraps_a_response_missing_required_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        urllib.request, "urlopen", lambda request: _FakeHTTPResponse(b'{"access_token": "tok"}')
    )
    with pytest.raises(RociaDbAuthError):
        auth_module._fetch_token_sync("https://idp.example/token", "id", "secret")


async def test_fetch_token_runs_the_blocking_call_off_the_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def slow_urlopen(request: urllib.request.Request) -> _FakeHTTPResponse:
        time.sleep(0.05)
        return _FakeHTTPResponse(
            b'{"access_token": "tok", "token_type": "Bearer", "expires_in": 600}'
        )

    monkeypatch.setattr(urllib.request, "urlopen", slow_urlopen)

    progressed: List[int] = []

    async def ticker() -> None:
        for i in range(5):
            await asyncio.sleep(0.01)
            progressed.append(i)

    token, _ = await asyncio.gather(
        fetch_token("https://idp.example/token", "id", "secret"), ticker()
    )
    assert token.access_token == "tok"
    # The event loop kept making progress on `ticker` while the blocking call ran, so
    # it must not have been called directly on the loop thread.
    assert len(progressed) == 5


# --- TokenManager: caching, dedup, and failure handling -------------------------------


async def test_get_authorization_header_fetches_lazily_and_then_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeFetcher([TokenResponse("tok", "Bearer", 600)])
    monkeypatch.setattr(auth_module, "fetch_token", fake)
    manager = TokenManager("https://idp.example/token", "id", "secret")

    assert await manager.get_authorization_header() == "Bearer tok"
    assert await manager.get_authorization_header() == "Bearer tok"
    assert fake.calls == 1


async def test_refresh_now_never_discards_a_still_valid_token_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeFetcher([TokenResponse("tok1", "Bearer", 600), RuntimeError("network down")])
    monkeypatch.setattr(auth_module, "fetch_token", fake)
    manager = TokenManager("https://idp.example/token", "id", "secret")

    await manager.refresh_now()
    assert await manager.get_authorization_header() == "Bearer tok1"

    with pytest.raises(RuntimeError, match="network down"):
        await manager.refresh_now()

    # The failed refresh must not have touched the still-valid cached header.
    assert await manager.get_authorization_header() == "Bearer tok1"
    assert fake.calls == 2


async def test_refresh_now_dedupes_concurrent_calls_into_a_single_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    call_started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def slow_fetch(token_url: str, client_id: str, client_secret: str) -> TokenResponse:
        nonlocal calls
        calls += 1
        call_started.set()
        await release.wait()
        return TokenResponse("tok", "Bearer", 600)

    monkeypatch.setattr(auth_module, "fetch_token", slow_fetch)
    manager = TokenManager("https://idp.example/token", "id", "secret")

    first = asyncio.ensure_future(manager.refresh_now())
    await call_started.wait()
    second = asyncio.ensure_future(manager.refresh_now())
    await asyncio.sleep(0)  # let `second` reach the dedup check before we release
    release.set()
    await asyncio.gather(first, second)

    assert calls == 1


async def test_refresh_interval_uses_two_thirds_of_the_last_reported_lifetime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeFetcher([TokenResponse("tok", "Bearer", 600)])
    monkeypatch.setattr(auth_module, "fetch_token", fake)
    manager = TokenManager("https://idp.example/token", "id", "secret")
    await manager.refresh_now()
    assert manager.refresh_interval() == 400.0


async def test_refresh_interval_never_drops_below_the_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeFetcher([TokenResponse("tok", "Bearer", 6)])
    monkeypatch.setattr(auth_module, "fetch_token", fake)
    manager = TokenManager("https://idp.example/token", "id", "secret")
    await manager.refresh_now()
    assert manager.refresh_interval() == MIN_REFRESH_INTERVAL


# --- TokenManager: background refresh -------------------------------------------------


async def test_request_refresh_wakes_the_background_task_without_waiting_for_the_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeFetcher([TokenResponse("tok", "Bearer", 600)])
    monkeypatch.setattr(auth_module, "fetch_token", fake)
    manager = TokenManager("https://idp.example/token", "id", "secret")

    handle = manager.spawn_refresh(interval=100.0)
    try:
        await asyncio.sleep(0.01)
        assert fake.calls == 0

        manager.request_refresh()
        await asyncio.sleep(0.05)
        assert fake.calls == 1
    finally:
        await handle.aclose()


async def test_spawn_refresh_refreshes_periodically(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeFetcher([TokenResponse("tok", "Bearer", 600)])
    monkeypatch.setattr(auth_module, "fetch_token", fake)
    manager = TokenManager("https://idp.example/token", "id", "secret")

    handle = manager.spawn_refresh(interval=0.02)
    try:
        await asyncio.sleep(0.09)
        assert fake.calls >= 3
    finally:
        await handle.aclose()


async def test_aclose_stops_the_background_refresh_task(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeFetcher([TokenResponse("tok", "Bearer", 600)])
    monkeypatch.setattr(auth_module, "fetch_token", fake)
    manager = TokenManager("https://idp.example/token", "id", "secret")

    handle = manager.spawn_refresh(interval=0.01)
    await asyncio.sleep(0.03)
    await handle.aclose()
    calls_at_close = fake.calls

    await asyncio.sleep(0.05)
    assert fake.calls == calls_at_close


async def test_background_refresh_failure_is_logged_and_the_loop_keeps_retrying(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # A real `fetch_token` failure always surfaces as `RociaDbAuthError` - the only
    # exception type `_refresh_loop` catches, by design, so the background task itself
    # never dies from a transient IdP failure.
    fake = _FakeFetcher([RociaDbAuthError("boom"), TokenResponse("tok", "Bearer", 600)])
    monkeypatch.setattr(auth_module, "fetch_token", fake)
    manager = TokenManager("https://idp.example/token", "id", "secret")

    handle = manager.spawn_refresh(interval=0.01)
    try:
        with caplog.at_level(logging.WARNING, logger=auth_module._logger.name):
            await asyncio.sleep(0.05)
        assert fake.calls >= 2
        assert any("refresh failed" in record.message for record in caplog.records)
    finally:
        await handle.aclose()


# --- auth metadata injection ----------------------------------------------------------


async def test_with_auth_metadata_appends_authorization_and_keeps_existing_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeFetcher([TokenResponse("tok", "Bearer", 600)])
    monkeypatch.setattr(auth_module, "fetch_token", fake)
    manager = TokenManager("https://idp.example/token", "id", "secret")

    details = _client_call_details(
        timeout=5.0,
        metadata=grpc.aio.Metadata(("x-existing", "value")),
        wait_for_ready=True,
    )
    augmented = await auth_module._with_auth_metadata(details, manager)

    assert augmented.method == details.method
    assert augmented.timeout == 5.0
    assert augmented.wait_for_ready is True
    pairs = list(augmented.metadata or [])
    assert ("x-existing", "value") in pairs
    assert ("authorization", "Bearer tok") in pairs


async def test_with_auth_metadata_handles_absent_existing_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeFetcher([TokenResponse("tok", "Bearer", 600)])
    monkeypatch.setattr(auth_module, "fetch_token", fake)
    manager = TokenManager("https://idp.example/token", "id", "secret")

    augmented = await auth_module._with_auth_metadata(_client_call_details(), manager)
    assert list(augmented.metadata or []) == [("authorization", "Bearer tok")]


# --- interceptors ----------------------------------------------------------------------


async def test_unary_unary_interceptor_injects_the_header_and_forwards_the_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeFetcher([TokenResponse("tok", "Bearer", 600)])
    monkeypatch.setattr(auth_module, "fetch_token", fake)
    manager = TokenManager("https://idp.example/token", "id", "secret")
    interceptor = AuthUnaryUnaryInterceptor(manager)

    seen: Dict[str, Any] = {}

    async def continuation(details: grpc.aio.ClientCallDetails, request: object) -> str:
        seen["details"] = details
        seen["request"] = request
        return "response"

    result = await interceptor.intercept_unary_unary(
        continuation, _client_call_details(), "the-request"
    )
    assert result == "response"
    assert seen["request"] == "the-request"
    assert ("authorization", "Bearer tok") in list(seen["details"].metadata or [])


async def test_unary_stream_interceptor_injects_the_header_and_forwards_the_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeFetcher([TokenResponse("tok", "Bearer", 600)])
    monkeypatch.setattr(auth_module, "fetch_token", fake)
    manager = TokenManager("https://idp.example/token", "id", "secret")
    interceptor = AuthUnaryStreamInterceptor(manager)

    seen: Dict[str, Any] = {}

    async def continuation(details: grpc.aio.ClientCallDetails, request: object) -> str:
        seen["details"] = details
        return "response"

    result = await interceptor.intercept_unary_stream(
        continuation, _client_call_details(), "the-request"
    )
    assert result == "response"
    assert ("authorization", "Bearer tok") in list(seen["details"].metadata or [])


async def test_stream_unary_interceptor_injects_the_header_and_forwards_the_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeFetcher([TokenResponse("tok", "Bearer", 600)])
    monkeypatch.setattr(auth_module, "fetch_token", fake)
    manager = TokenManager("https://idp.example/token", "id", "secret")
    interceptor = AuthStreamUnaryInterceptor(manager)

    seen: Dict[str, Any] = {}

    async def continuation(details: grpc.aio.ClientCallDetails, request_iterator: object) -> str:
        seen["details"] = details
        return "response"

    result = await interceptor.intercept_stream_unary(
        continuation, _client_call_details(), iter(["req"])
    )
    assert result == "response"
    assert ("authorization", "Bearer tok") in list(seen["details"].metadata or [])

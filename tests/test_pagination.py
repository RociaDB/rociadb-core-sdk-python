"""Unit tests for the pure pagination helpers shared by every listing RPC."""

from __future__ import annotations

import pytest

from rocia_db_sdk._pagination import (
    DEFAULT_PAGE_SIZE,
    _next_pagination_cursor,
    _optional_cursor,
    _page_request,
)
from rocia_db_sdk.errors import RociaDbValidationError


def test_page_request_defaults_limit_and_hides_empty_cursor() -> None:
    page = _page_request(None, None)
    assert page.limit == DEFAULT_PAGE_SIZE
    assert page.cursor == ""


def test_page_request_forwards_an_explicit_limit_and_cursor_unchanged() -> None:
    page = _page_request(5, "cursor-1")
    assert page.limit == 5
    assert page.cursor == "cursor-1"


def test_page_request_does_not_hardcode_the_servers_page_size_ceiling() -> None:
    # The server's own ceiling (200 by default) is configurable server-side; the SDK
    # must forward any positive limit unchanged, including one above 200.
    page = _page_request(10_000, None)
    assert page.limit == 10_000


def test_page_request_rejects_a_zero_limit_before_any_rpc() -> None:
    with pytest.raises(RociaDbValidationError):
        _page_request(0, None)


def test_page_request_rejects_a_negative_limit_before_any_rpc() -> None:
    with pytest.raises(RociaDbValidationError):
        _page_request(-1, None)


def test_optional_cursor_maps_empty_string_to_none() -> None:
    assert _optional_cursor("") is None


def test_optional_cursor_keeps_a_non_empty_cursor() -> None:
    assert _optional_cursor("next") == "next"


def test_next_pagination_cursor_stops_when_next_cursor_is_absent() -> None:
    assert _next_pagination_cursor(None, None) is None
    assert _next_pagination_cursor("cursor-1", None) is None


def test_next_pagination_cursor_continues_on_a_fresh_cursor_even_from_the_first_page() -> None:
    assert _next_pagination_cursor(None, "cursor-1") == "cursor-1"


def test_next_pagination_cursor_continues_on_any_fresh_cursor() -> None:
    assert _next_pagination_cursor("cursor-1", "cursor-2") == "cursor-2"


def test_next_pagination_cursor_stops_on_a_repeated_cursor() -> None:
    # Guards against looping forever if a misbehaving server echoes the same cursor.
    assert _next_pagination_cursor("cursor-1", "cursor-1") is None

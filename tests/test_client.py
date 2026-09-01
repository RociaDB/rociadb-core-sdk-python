"""Unit tests for `_resolve_endpoint`, `_resolve_connect_timeout`, and `RociaDbBuilder`.

Everything here is pure/offline: no channel is ever built and no network call is ever
made. `RociaDbClient.connect`/`RociaDbBuilder.build` themselves need a live server and
are exercised elsewhere; this file targets exactly the validation logic that must run
- and fail loudly - before any of that.
"""

from __future__ import annotations

import pytest

from rocia_db_sdk.client import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_HOST,
    RociaDbBuilder,
    _resolve_connect_timeout,
    _resolve_endpoint,
)
from rocia_db_sdk.errors import RociaDbConnectionError, RociaDbValidationError

# --- _resolve_endpoint: scheme selects TLS, missing port takes the scheme default ----


def test_resolve_endpoint_https_without_a_port_defaults_to_443_and_enables_tls() -> None:
    target, use_tls = _resolve_endpoint("https://db.example.com")
    assert target == "db.example.com:443"
    assert use_tls is True


def test_resolve_endpoint_https_with_an_explicit_443_matches_the_default() -> None:
    target, use_tls = _resolve_endpoint("https://db.example.com:443")
    assert target == "db.example.com:443"
    assert use_tls is True


def test_resolve_endpoint_http_without_a_port_defaults_to_80_and_disables_tls() -> None:
    target, use_tls = _resolve_endpoint("http://db.example.com")
    assert target == "db.example.com:80"
    assert use_tls is False


def test_resolve_endpoint_http_with_an_arbitrary_port_disables_tls() -> None:
    target, use_tls = _resolve_endpoint("http://127.0.0.1:50051")
    assert target == "127.0.0.1:50051"
    assert use_tls is False


def test_resolve_endpoint_https_with_a_non_default_port_keeps_it() -> None:
    target, use_tls = _resolve_endpoint("https://db.example.com:8443")
    assert target == "db.example.com:8443"
    assert use_tls is True


def test_resolve_endpoint_rejects_a_url_carrying_a_path() -> None:
    with pytest.raises(RociaDbConnectionError):
        _resolve_endpoint("http://127.0.0.1:50051/some/path")


def test_resolve_endpoint_accepts_a_bare_root_path() -> None:
    # A trailing "/" and no path at all are equally "no path".
    target, _ = _resolve_endpoint("http://127.0.0.1:50051/")
    assert target == "127.0.0.1:50051"


def test_resolve_endpoint_rejects_an_unsupported_scheme() -> None:
    with pytest.raises(RociaDbConnectionError):
        _resolve_endpoint("ftp://127.0.0.1:50051")


def test_resolve_endpoint_rejects_a_missing_hostname() -> None:
    with pytest.raises(RociaDbConnectionError):
        _resolve_endpoint("http://")


def test_default_host_is_the_documented_local_default() -> None:
    assert DEFAULT_HOST == "http://127.0.0.1:50051"


# --- _resolve_connect_timeout ---------------------------------------------------------


def test_resolve_connect_timeout_defaults_when_none_given() -> None:
    assert _resolve_connect_timeout(None) == DEFAULT_CONNECT_TIMEOUT == 10.0


def test_resolve_connect_timeout_forwards_a_positive_explicit_value() -> None:
    assert _resolve_connect_timeout(5.0) == 5.0


def test_resolve_connect_timeout_rejects_zero() -> None:
    with pytest.raises(RociaDbValidationError):
        _resolve_connect_timeout(0)


def test_resolve_connect_timeout_rejects_a_negative_value() -> None:
    with pytest.raises(RociaDbValidationError):
        _resolve_connect_timeout(-1.0)


# --- RociaDbBuilder: fluent chaining and eager connect_timeout validation -------------


def test_builder_host_and_auth_client_credentials_chain_fluently() -> None:
    builder = RociaDbBuilder()
    assert builder.host("https://db.example.com") is builder
    assert builder.auth_client_credentials("https://idp/token", "id", "secret") is builder
    assert builder.disable_auth() is builder
    assert builder.connect_timeout(5.0) is builder


def test_builder_connect_timeout_validates_immediately_not_deferred_to_build() -> None:
    builder = RociaDbBuilder()
    with pytest.raises(RociaDbValidationError):
        builder.connect_timeout(-1.0)


def test_builder_connect_timeout_rejects_zero_immediately() -> None:
    with pytest.raises(RociaDbValidationError):
        RociaDbBuilder().connect_timeout(0)


def test_builder_auth_client_credentials_clears_a_prior_disable_auth() -> None:
    builder = RociaDbBuilder()
    builder.disable_auth()
    builder.auth_client_credentials("https://idp/token", "id", "secret")
    assert builder._auth_disabled is False

"""Unit tests for the RociaDbError hierarchy and its JSON translators."""

from __future__ import annotations

from typing import Optional

import grpc
import grpc.aio
import pytest

from rociadb_sdk.errors import (
    RociaDbAuthError,
    RociaDbConnectionError,
    RociaDbDecodeError,
    RociaDbEncodeError,
    RociaDbError,
    RociaDbStatusError,
    RociaDbValidationError,
    _decode_json,
    _encode_json,
    _status_error,
)


def _rpc_error(
    code: grpc.StatusCode, details: str = "boom", reason: Optional[str] = "not_found"
) -> grpc.aio.AioRpcError:
    trailing = grpc.aio.Metadata(("reason", reason)) if reason is not None else grpc.aio.Metadata()
    return grpc.aio.AioRpcError(code=code, trailing_metadata=trailing, details=details)


def test_status_error_exposes_code_reason_and_grpc_error() -> None:
    grpc_error = _rpc_error(grpc.StatusCode.NOT_FOUND, "document not found", "not_found")
    error = _status_error("failed to get document", grpc_error)

    assert isinstance(error, RociaDbStatusError)
    assert error.operation == "failed to get document"
    assert error.code == grpc.StatusCode.NOT_FOUND
    assert error.reason == "not_found"
    assert error.grpc_error is grpc_error
    message = str(error)
    assert "failed to get document" in message
    assert "document not found" in message


def test_status_error_without_reason_metadata_reports_none() -> None:
    grpc_error = _rpc_error(grpc.StatusCode.INTERNAL, "boom", reason=None)
    error = _status_error("failed to do something", grpc_error)
    assert error.reason is None


def test_is_unauthenticated_true_only_for_unauthenticated_status() -> None:
    error = _status_error("failed to list documents", _rpc_error(grpc.StatusCode.UNAUTHENTICATED))
    assert error.is_unauthenticated() is True
    assert error.is_permission_denied() is False


def test_is_permission_denied_true_only_for_permission_denied_status() -> None:
    error = _status_error(
        "failed to delete document", _rpc_error(grpc.StatusCode.PERMISSION_DENIED)
    )
    assert error.is_permission_denied() is True
    assert error.is_unauthenticated() is False


def test_is_unauthenticated_and_is_permission_denied_are_false_for_other_codes() -> None:
    error = _status_error("failed to get node", _rpc_error(grpc.StatusCode.NOT_FOUND))
    assert error.is_unauthenticated() is False
    assert error.is_permission_denied() is False


def test_non_status_variants_default_both_predicates_to_false() -> None:
    for error in (
        RociaDbValidationError("page limit must be greater than zero"),
        RociaDbConnectionError("invalid host URL"),
        RociaDbAuthError("token endpoint returned error"),
        RociaDbEncodeError("document json"),
        RociaDbDecodeError("document json"),
    ):
        assert isinstance(error, RociaDbError)
        assert error.is_unauthenticated() is False
        assert error.is_permission_denied() is False


def test_connection_error_does_not_shadow_the_builtin() -> None:
    assert not issubclass(RociaDbConnectionError, ConnectionError)
    assert issubclass(RociaDbConnectionError, RociaDbError)


def test_encode_and_decode_error_carry_their_context() -> None:
    encode_error = RociaDbEncodeError("document json")
    assert encode_error.context == "document json"
    assert "document json" in str(encode_error)

    decode_error = RociaDbDecodeError("node json")
    assert decode_error.context == "node json"
    assert "node json" in str(decode_error)


def test_encode_json_round_trips_plain_values() -> None:
    assert _encode_json({"a": 1}, "document json") == b'{"a": 1}'


def test_encode_json_wraps_a_value_that_cannot_be_serialized() -> None:
    class Unserializable:
        pass

    with pytest.raises(RociaDbEncodeError) as excinfo:
        _encode_json(Unserializable(), "document json")
    assert excinfo.value.context == "document json"


def test_decode_json_round_trips_plain_values() -> None:
    assert _decode_json(b'{"a": 1}', "document json") == {"a": 1}


def test_decode_json_wraps_invalid_json() -> None:
    with pytest.raises(RociaDbDecodeError) as excinfo:
        _decode_json(b"{ not valid json", "document json")
    assert excinfo.value.context == "document json"


def test_decode_json_wraps_invalid_utf8() -> None:
    with pytest.raises(RociaDbDecodeError):
        _decode_json(b"\xff\xfe", "document json")

"""Error surface raised by every fallible RociaDbClient call.

Discriminated as an exception class hierarchy (`isinstance`/`except` on a subclass)
rather than one exception carrying a discriminant field: that is the idiomatic Python
substitute for a tagged union, and it keeps a bare ``except RociaDbError`` working for
callers who only care that *something* failed. :meth:`RociaDbError.is_unauthenticated`
and :meth:`RociaDbError.is_permission_denied` live on the base class (returning
``False``) and are overridden only by :class:`RociaDbStatusError`, so either predicate
is safe to call on any caught error without matching the subclass first.
"""

from __future__ import annotations

import json
from typing import Any, Awaitable, Optional, TypeVar

import grpc
import grpc.aio


class RociaDbError(Exception):
    """Base exception raised by every fallible RociaDbClient call."""

    def is_unauthenticated(self) -> bool:
        """True when the server rejected the call as unauthenticated.

        The server treats this as a renewal signal: refresh the auth token and retry.
        Only :class:`RociaDbStatusError` can ever answer ``True``; every other subclass
        keeps this base implementation's ``False``.
        """
        return False

    def is_permission_denied(self) -> bool:
        """True when the server rejected the call for lacking permission.

        Unlike :meth:`is_unauthenticated`, this is final: the token is valid but lacks
        the required scope, and refreshing it will not help.
        """
        return False


class RociaDbStatusError(RociaDbError):
    """A gRPC call to the upstream server returned a non-OK status.

    `reason` carries the server's ``reason`` trailing metadata value (one of
    ``invalid_argument``, ``not_found``, ``already_exists``, ``permission_denied``,
    ``unauthenticated``, ``internal``) - finer-grained than `code` alone. It is `None`
    only when the response carried no such metadata.
    """

    def __init__(self, operation: str, grpc_error: grpc.aio.AioRpcError) -> None:
        self.operation = operation
        self.grpc_error = grpc_error
        self.code: grpc.StatusCode = grpc_error.code()
        self.reason: Optional[str] = _extract_reason(grpc_error)
        detail = grpc_error.details() or str(self.code)
        super().__init__(f"{operation}: {detail}")

    def is_unauthenticated(self) -> bool:
        return bool(self.code == grpc.StatusCode.UNAUTHENTICATED)

    def is_permission_denied(self) -> bool:
        return bool(self.code == grpc.StatusCode.PERMISSION_DENIED)


def _extract_reason(grpc_error: grpc.aio.AioRpcError) -> Optional[str]:
    metadata = grpc_error.trailing_metadata()
    if not metadata:
        return None
    for key, value in metadata:
        if key != "reason":
            continue
        return value if isinstance(value, str) else value.decode("utf-8")
    return None


class RociaDbConnectionError(RociaDbError):
    """Failed to connect to, or configure, the upstream endpoint.

    Named `RociaDbConnectionError` rather than `ConnectionError` so it does not shadow
    the Python builtin exception of that name.
    """


class RociaDbAuthError(RociaDbError):
    """Failed to obtain or refresh the upstream OAuth2 token."""


class RociaDbEncodeError(RociaDbError):
    """Failed to JSON-encode a value before sending it upstream."""

    def __init__(self, context: str, message: Optional[str] = None) -> None:
        self.context = context
        super().__init__(message or f"failed to encode {context}")


class RociaDbDecodeError(RociaDbError):
    """Failed to JSON-decode a payload received from upstream."""

    def __init__(self, context: str, message: Optional[str] = None) -> None:
        self.context = context
        super().__init__(message or f"failed to decode {context}")


class RociaDbValidationError(RociaDbError):
    """A client-side rule was violated before any network call was made."""


def _status_error(operation: str, grpc_error: grpc.aio.AioRpcError) -> RociaDbStatusError:
    """Wrap a failed unary or streaming gRPC call into `RociaDbStatusError`."""
    return RociaDbStatusError(operation, grpc_error)


def _encode_json(value: Any, context: str) -> bytes:
    """JSON-encode `value` as UTF-8 bytes, wrapping any failure into `RociaDbEncodeError`."""
    try:
        return json.dumps(value).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RociaDbEncodeError(context) from exc


def _decode_json(data: bytes, context: str) -> Any:
    """JSON-decode UTF-8 `data`, wrapping any failure into `RociaDbDecodeError`."""
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RociaDbDecodeError(context) from exc


_R = TypeVar("_R")


async def _call(operation: str, call: Awaitable[_R]) -> _R:
    """Await one unary gRPC `call`, translating a failed status into `RociaDbStatusError`.

    Shared by every RPC method across the four service mixins so each one wraps its
    stub invocation the same way instead of repeating the same try/except.
    """
    try:
        return await call
    except grpc.aio.AioRpcError as exc:
        raise _status_error(operation, exc) from exc

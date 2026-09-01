"""Pure pagination helpers shared by every listing RPC across the four services.

Kept in one module rather than duplicated per service file (or folded into the client)
because `document.py`, `graph.py`, `file.py`, and `tenant.py` all need the identical
three functions.
"""

from __future__ import annotations

from typing import Optional

from rocia_db_sdk._pb.upstream.v1 import upstream_pb2 as pb
from rocia_db_sdk.errors import RociaDbValidationError

#: Applied whenever a caller omits `limit` on a paginated call.
DEFAULT_PAGE_SIZE = 20


def _page_request(limit: Optional[int], cursor: Optional[str]) -> pb.PageRequest:
    """Build the `PageRequest` for a listing RPC, applying `DEFAULT_PAGE_SIZE`.

    Rejects a non-positive `limit` with `RociaDbValidationError` before any RPC - the
    server does the same for zero, but only after a round trip. The server's own
    page-size ceiling (`limits.max_page_size`, 200 by default) is intentionally not
    duplicated here: it is configurable server-side, so any positive limit is forwarded
    unchanged and the server has the final say.
    """
    if limit is not None and limit <= 0:
        raise RociaDbValidationError(f"page limit must be greater than zero, got {limit}")
    return pb.PageRequest(
        limit=limit if limit is not None else DEFAULT_PAGE_SIZE, cursor=cursor or ""
    )


def _optional_cursor(cursor: str) -> Optional[str]:
    """Map the protobuf empty-string cursor (`""`, meaning "no further page") to `None`."""
    return cursor or None


def _next_pagination_cursor(
    current_cursor: Optional[str], next_cursor: Optional[str]
) -> Optional[str]:
    """Decide whether a page-following loop should keep going.

    Continues on any fresh cursor - including one attached to an empty or
    shorter-than-`limit` page, since the server can legitimately hand back a short or
    empty page mid-listing (e.g. an index entry surviving a deleted document) followed
    by more data. Stops when `next_cursor` is absent, or when the server repeats the
    cursor just used - a guard against looping forever on a misbehaving server.
    """
    if not next_cursor or next_cursor == current_cursor:
        return None
    return next_cursor

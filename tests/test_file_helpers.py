"""Unit tests for the pure checksum/size validation and upload-chunking helpers."""

from __future__ import annotations

import hashlib
from typing import AsyncIterator, List, TypeVar

import pytest

from rocia_db_sdk.errors import RociaDbValidationError
from rocia_db_sdk.file import (
    CHECKSUM_LEN,
    MAX_FILE_BYTES,
    UPLOAD_CHUNK_BYTES,
    _chunk_upload_requests,
    _rechunk_upload_requests,
    _require_checksum_len,
    _resolve_checksum,
    _validate_file_size,
)

# --- _validate_file_size ----------------------------------------------------------


def test_validate_file_size_accepts_exactly_the_ceiling() -> None:
    _validate_file_size(MAX_FILE_BYTES)


def test_validate_file_size_rejects_one_byte_over_the_ceiling() -> None:
    with pytest.raises(RociaDbValidationError):
        _validate_file_size(MAX_FILE_BYTES + 1)


# --- _require_checksum_len / _resolve_checksum -----------------------------------


def test_require_checksum_len_accepts_exactly_32_bytes() -> None:
    _require_checksum_len(b"0" * CHECKSUM_LEN)


def test_require_checksum_len_rejects_any_other_length() -> None:
    with pytest.raises(RociaDbValidationError):
        _require_checksum_len(b"0" * (CHECKSUM_LEN - 1))
    with pytest.raises(RociaDbValidationError):
        _require_checksum_len(b"0" * (CHECKSUM_LEN + 1))


def test_resolve_checksum_computes_sha256_when_omitted() -> None:
    data = b"hello world"
    assert _resolve_checksum(None, data) == hashlib.sha256(data).digest()


def test_resolve_checksum_matches_a_known_sha256_digest() -> None:
    # A hardcoded, independently verifiable digest - not just a round trip through the
    # same hashlib call the implementation itself uses.
    known_hex = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    digest = _resolve_checksum(None, b"hello world")
    assert digest.hex() == known_hex
    assert len(digest) == CHECKSUM_LEN


def test_resolve_checksum_accepts_a_supplied_32_byte_checksum() -> None:
    checksum = b"0" * CHECKSUM_LEN
    assert _resolve_checksum(checksum, b"irrelevant") == checksum


def test_resolve_checksum_rejects_a_supplied_checksum_of_the_wrong_length() -> None:
    with pytest.raises(RociaDbValidationError):
        _resolve_checksum(b"too-short", b"irrelevant")


# --- _chunk_upload_requests --------------------------------------------------------


def test_chunk_upload_requests_on_empty_data_yields_one_metadata_only_message() -> None:
    requests = list(
        _chunk_upload_requests("t1", "bucket", "file1", b"", "text/plain", b"cs", "req-1")
    )
    assert len(requests) == 1
    only = requests[0]
    assert only.tenant_id == "t1"
    assert only.bucket == "bucket"
    assert only.file_id == "file1"
    assert only.size_bytes == 0
    assert only.content_type == "text/plain"
    assert only.checksum == b"cs"
    assert only.chunk == b""
    assert only.request_id == "req-1"


def test_chunk_upload_requests_splits_into_exactly_one_mib_pieces() -> None:
    data = b"x" * (UPLOAD_CHUNK_BYTES * 2 + 10)
    requests = list(
        _chunk_upload_requests("t1", "bucket", "file1", data, "text/plain", b"cs", "req-1")
    )
    assert len(requests) == 3
    assert [len(r.chunk) for r in requests] == [UPLOAD_CHUNK_BYTES, UPLOAD_CHUNK_BYTES, 10]
    assert b"".join(r.chunk for r in requests) == data


def test_chunk_upload_requests_attaches_metadata_only_to_the_first_message() -> None:
    data = b"x" * (UPLOAD_CHUNK_BYTES + 1)
    requests = list(
        _chunk_upload_requests("t1", "bucket", "file1", data, "text/plain", b"cs", "req-1")
    )
    first, second = requests
    assert first.tenant_id == "t1"
    assert first.size_bytes == len(data)
    assert first.request_id == "req-1"
    assert second.tenant_id == ""
    assert second.bucket == ""
    assert second.file_id == ""
    assert second.size_bytes == 0
    assert second.content_type == ""
    assert second.checksum == b""
    assert second.request_id == ""


def test_chunk_upload_requests_preserves_a_data_length_exactly_a_multiple_of_the_chunk_size() -> (
    None
):
    data = b"y" * (UPLOAD_CHUNK_BYTES * 2)
    requests = list(
        _chunk_upload_requests("t1", "bucket", "file1", data, "text/plain", b"cs", "req-1")
    )
    assert [len(r.chunk) for r in requests] == [UPLOAD_CHUNK_BYTES, UPLOAD_CHUNK_BYTES]


@pytest.mark.parametrize(
    "size",
    [
        0,
        1,
        UPLOAD_CHUNK_BYTES - 1,
        UPLOAD_CHUNK_BYTES,
        UPLOAD_CHUNK_BYTES + 1,
        int(UPLOAD_CHUNK_BYTES * 2.5),
    ],
    ids=["0", "1", "chunk-1", "chunk", "chunk+1", "~2.5-chunks"],
)
def test_chunk_upload_requests_produces_exactly_1mib_pieces_except_the_last_for_every_size(
    size: int,
) -> None:
    data = bytes(i % 256 for i in range(size))
    requests = list(
        _chunk_upload_requests("t1", "bucket", "file1", data, "text/plain", b"cs", "req-1")
    )

    lengths = [len(r.chunk) for r in requests]
    if lengths:
        assert all(length == UPLOAD_CHUNK_BYTES for length in lengths[:-1])
        assert 0 < lengths[-1] <= UPLOAD_CHUNK_BYTES or (size == 0 and lengths == [0])
    assert sum(lengths) == size
    assert b"".join(r.chunk for r in requests) == data


# --- _rechunk_upload_requests -------------------------------------------------------

_T = TypeVar("_T")


async def _collect(aiter: AsyncIterator[_T]) -> List[_T]:
    return [item async for item in aiter]


async def _to_async_iter(pieces: List[bytes]) -> AsyncIterator[bytes]:
    for piece in pieces:
        yield piece


async def test_rechunk_upload_requests_on_a_zero_byte_source_yields_one_empty_message() -> None:
    requests = await _collect(
        _rechunk_upload_requests("t1", "b", "f1", 0, "text/plain", b"cs", "req-1", [])
    )
    assert len(requests) == 1
    assert requests[0].chunk == b""
    assert requests[0].size_bytes == 0


async def test_rechunk_upload_requests_regroups_arbitrarily_sized_pieces_into_1mib_chunks() -> None:
    total = UPLOAD_CHUNK_BYTES * 2 + 10
    # Deliberately misaligned with the 1 MiB boundary: three uneven source pieces.
    pieces = [b"a" * 100, b"b" * (total - 200), b"c" * 100]
    assert sum(len(p) for p in pieces) == total

    requests = await _collect(
        _rechunk_upload_requests("t1", "b", "f1", total, "text/plain", b"cs", "req-1", pieces)
    )
    lengths = [len(r.chunk) for r in requests]
    assert lengths == [UPLOAD_CHUNK_BYTES, UPLOAD_CHUNK_BYTES, 10]
    assert b"".join(r.chunk for r in requests) == b"".join(pieces)


async def test_rechunk_upload_requests_attaches_metadata_only_to_the_first_message() -> None:
    total = UPLOAD_CHUNK_BYTES + 1
    requests = await _collect(
        _rechunk_upload_requests(
            "t1", "b", "f1", total, "text/plain", b"cs", "req-1", [b"z" * total]
        )
    )
    first, second = requests
    assert first.tenant_id == "t1"
    assert first.size_bytes == total
    assert second.tenant_id == ""


async def test_rechunk_upload_requests_accepts_an_async_source() -> None:
    total = 10
    requests = await _collect(
        _rechunk_upload_requests(
            "t1", "b", "f1", total, "text/plain", b"cs", "req-1", _to_async_iter([b"x" * total])
        )
    )
    assert len(requests) == 1
    assert requests[0].chunk == b"x" * total


async def test_rechunk_upload_requests_rejects_more_data_than_size_bytes_declared() -> None:
    with pytest.raises(RociaDbValidationError):
        await _collect(
            _rechunk_upload_requests("t1", "b", "f1", 5, "text/plain", b"cs", "req-1", [b"x" * 10])
        )


async def test_rechunk_upload_requests_rejects_a_source_left_short_of_size_bytes() -> None:
    with pytest.raises(RociaDbValidationError):
        await _collect(
            _rechunk_upload_requests("t1", "b", "f1", 10, "text/plain", b"cs", "req-1", [b"x" * 5])
        )


@pytest.mark.parametrize(
    "size",
    [
        0,
        1,
        UPLOAD_CHUNK_BYTES - 1,
        UPLOAD_CHUNK_BYTES,
        UPLOAD_CHUNK_BYTES + 1,
        int(UPLOAD_CHUNK_BYTES * 2.5),
    ],
    ids=["0", "1", "chunk-1", "chunk", "chunk+1", "~2.5-chunks"],
)
async def test_rechunk_upload_requests_produces_exactly_1mib_pieces_except_the_last_for_every_size(
    size: int,
) -> None:
    data = bytes(i % 256 for i in range(size))
    requests = await _collect(
        _rechunk_upload_requests("t1", "b", "f1", size, "text/plain", b"cs", "req-1", [data])
    )

    lengths = [len(r.chunk) for r in requests]
    if lengths:
        assert all(length == UPLOAD_CHUNK_BYTES for length in lengths[:-1])
    assert sum(lengths) == size
    assert b"".join(r.chunk for r in requests) == data

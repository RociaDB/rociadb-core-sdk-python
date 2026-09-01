"""Unit tests for `_FileMixin`'s RPC methods, against fake (no-network) service stubs.

Pure upload-chunking/checksum helpers are covered separately in test_file_helpers.py;
this file exercises the RPC-calling methods themselves: request construction, response
decoding, streaming download cancellation, and default `request_id` generation.
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Any, AsyncGenerator, AsyncIterator, List, cast

import grpc
import grpc.aio
import pytest

from _doubles import FakeDownloadStub, FakeStreamUnaryCall, FakeUnaryCall
from rocia_db_sdk._pb.upstream.v1 import upstream_pb2 as pb
from rocia_db_sdk.errors import RociaDbStatusError
from rocia_db_sdk.file import CHECKSUM_LEN, UPLOAD_CHUNK_BYTES, _FileMixin
from rocia_db_sdk.types import RawUploadMessage


class _FakeFilesStub:
    def __init__(self) -> None:
        self.Upload: Any = None
        self.Download: Any = None
        self.Stat: Any = None
        self.Delete: Any = None
        self.ListBuckets: Any = None
        self.ListFiles: Any = None


class _Client(_FileMixin):
    def __init__(self, files: Any) -> None:
        self._files = files


# --- upload_file: tier 1 (in-memory buffer) -----------------------------------------


async def test_upload_file_sends_the_data_chunked_with_metadata_on_the_first_message() -> None:
    upload = FakeStreamUnaryCall().returns(None)
    stub = _FakeFilesStub()
    stub.Upload = upload
    client = _Client(stub)
    data = b"x" * (UPLOAD_CHUNK_BYTES + 10)

    await client.upload_file("t1", "bucket", "file-1", data, content_type="text/plain")

    assert len(upload.received) == 2
    first, second = upload.received
    assert first.tenant_id == "t1"
    assert first.size_bytes == len(data)
    assert first.content_type == "text/plain"
    assert first.checksum == hashlib.sha256(data).digest()
    assert first.request_id.startswith("upload_file:")
    uuid.UUID(first.request_id.split(":", 1)[1])
    assert b"".join(r.chunk for r in upload.received) == data
    assert second.tenant_id == ""  # metadata only on the first message


async def test_upload_file_accepts_a_supplied_checksum_and_request_id() -> None:
    upload = FakeStreamUnaryCall().returns(None)
    stub = _FakeFilesStub()
    stub.Upload = upload
    client = _Client(stub)
    checksum = b"0" * CHECKSUM_LEN

    await client.upload_file(
        "t1", "bucket", "file-1", b"data", checksum=checksum, request_id="my-id"
    )

    assert upload.received[0].checksum == checksum
    assert upload.received[0].request_id == "my-id"


# --- upload_file_chunked: tier 2 (assisted re-chunking) ------------------------------


async def test_upload_file_chunked_forwards_the_re_chunked_stream() -> None:
    upload = FakeStreamUnaryCall().returns(None)
    stub = _FakeFilesStub()
    stub.Upload = upload
    client = _Client(stub)
    checksum = b"1" * CHECKSUM_LEN

    await client.upload_file_chunked(
        "t1", "bucket", "file-1", 4, checksum, [b"ab", b"cd"], content_type="text/plain"
    )

    assert len(upload.received) == 1
    assert upload.received[0].chunk == b"abcd"
    assert upload.received[0].checksum == checksum
    assert upload.received[0].request_id.startswith("upload_file:")


# --- upload_file_raw: tier 3 (zero-validation escape hatch) --------------------------


async def test_upload_file_raw_sends_every_message_exactly_as_given() -> None:
    upload = FakeStreamUnaryCall().returns(None)
    stub = _FakeFilesStub()
    stub.Upload = upload
    client = _Client(stub)
    messages = [
        RawUploadMessage(
            tenant_id="t1",
            bucket="b",
            file_id="f",
            size_bytes=3,
            content_type="text/plain",
            checksum=b"2" * CHECKSUM_LEN,
            chunk=b"abc",
            request_id="raw-1",
        )
    ]

    await client.upload_file_raw(messages)

    assert len(upload.received) == 1
    sent = upload.received[0]
    assert sent.tenant_id == "t1"
    assert sent.chunk == b"abc"
    assert sent.request_id == "raw-1"


async def test_upload_file_raw_accepts_an_async_source() -> None:
    upload = FakeStreamUnaryCall().returns(None)
    stub = _FakeFilesStub()
    stub.Upload = upload
    client = _Client(stub)

    async def messages() -> AsyncIterator[RawUploadMessage]:
        yield RawUploadMessage(
            tenant_id="t1",
            bucket="b",
            file_id="f",
            size_bytes=1,
            content_type="",
            checksum=b"",
            chunk=b"x",
            request_id="raw-2",
        )

    await client.upload_file_raw(messages())

    assert upload.received[0].chunk == b"x"


# --- download_file_stream / download_file --------------------------------------------


async def test_download_file_stream_yields_chunks_lazily() -> None:
    download = FakeDownloadStub(
        chunks=[pb.DownloadResponse(chunk=b"ab"), pb.DownloadResponse(chunk=b"cd")]
    )
    stub = _FakeFilesStub()
    stub.Download = download
    client = _Client(stub)

    chunks = [c async for c in client.download_file_stream("t1", "b", "f1")]

    assert chunks == [b"ab", b"cd"]
    request = download.requests[0]
    assert (request.tenant_id, request.bucket, request.file_id) == ("t1", "b", "f1")


async def test_download_file_joins_every_chunk() -> None:
    download = FakeDownloadStub(
        chunks=[pb.DownloadResponse(chunk=b"ab"), pb.DownloadResponse(chunk=b"cd")]
    )
    stub = _FakeFilesStub()
    stub.Download = download
    client = _Client(stub)

    assert await client.download_file("t1", "b", "f1") == b"abcd"


async def test_download_file_stream_cancels_the_call_when_the_consumer_stops_early() -> None:
    download = FakeDownloadStub(
        chunks=[
            pb.DownloadResponse(chunk=b"a"),
            pb.DownloadResponse(chunk=b"b"),
            pb.DownloadResponse(chunk=b"c"),
        ]
    )
    stub = _FakeFilesStub()
    stub.Download = download
    client = _Client(stub)

    # A generator's `finally` only fires once it is actually closed. Python's `async
    # for ... break` does not guarantee that synchronously (unlike JS `for await`) - it
    # relies on the interpreter's refcounting-triggered finalizer, scheduled onto a
    # later event-loop tick rather than run in place. Closing the generator explicitly
    # is the deterministic way an early-exiting consumer should stop it.
    generator = cast("AsyncGenerator[bytes, None]", client.download_file_stream("t1", "b", "f1"))
    collected: List[bytes] = []
    async for chunk in generator:
        collected.append(chunk)
        if len(collected) == 1:
            break
    await generator.aclose()

    assert collected == [b"a"]
    assert download.cancelled is True


async def test_download_file_stream_wraps_a_mid_stream_rpc_failure() -> None:
    grpc_error = grpc.aio.AioRpcError(
        code=grpc.StatusCode.NOT_FOUND,
        trailing_metadata=grpc.aio.Metadata(("reason", "not_found")),
        details="file not found",
    )
    download = FakeDownloadStub(chunks=[pb.DownloadResponse(chunk=b"a")]).raises_after_chunks(
        grpc_error
    )
    stub = _FakeFilesStub()
    stub.Download = download
    client = _Client(stub)

    with pytest.raises(RociaDbStatusError) as excinfo:
        async for _ in client.download_file_stream("t1", "b", "f1"):
            pass

    assert excinfo.value.code == grpc.StatusCode.NOT_FOUND
    assert download.cancelled is True


# --- stat_file / delete_file / list_buckets / list_files ------------------------------


async def test_stat_file_maps_every_field() -> None:
    stat = FakeUnaryCall().returns(
        pb.StatResponse(
            size_bytes=1234,
            content_type="text/plain",
            checksum=b"3" * CHECKSUM_LEN,
            created_at="2026-01-01T00:00:00Z",
            updated_at="2026-01-02T00:00:00Z",
        )
    )
    stub = _FakeFilesStub()
    stub.Stat = stat
    client = _Client(stub)

    metadata = await client.stat_file("t1", "b", "f1")

    assert metadata.size_bytes == 1234
    assert metadata.content_type == "text/plain"
    assert metadata.checksum == b"3" * CHECKSUM_LEN
    assert metadata.created_at == "2026-01-01T00:00:00Z"
    assert metadata.updated_at == "2026-01-02T00:00:00Z"


async def test_delete_file_defaults_request_id_to_delete_file_uuid() -> None:
    delete = FakeUnaryCall().returns(None)
    stub = _FakeFilesStub()
    stub.Delete = delete
    client = _Client(stub)

    await client.delete_file("t1", "b", "f1")

    assert delete.requests[0].request_id.startswith("delete_file:")


async def test_list_buckets_and_list_files_decode_plain_string_pages() -> None:
    list_buckets = FakeUnaryCall().returns(
        pb.ListBucketsResponse(buckets=["b1"], page=pb.PageResponse(next_cursor=""))
    )
    list_files = FakeUnaryCall().returns(
        pb.ListFilesResponse(file_ids=["f1", "f2"], page=pb.PageResponse(next_cursor=""))
    )
    stub = _FakeFilesStub()
    stub.ListBuckets = list_buckets
    stub.ListFiles = list_files
    client = _Client(stub)

    assert (await client.list_buckets("t1")).items == ["b1"]
    assert (await client.list_files("t1", "b1")).items == ["f1", "f2"]

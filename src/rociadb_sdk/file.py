"""FileService RPC methods and the pure, network-free helpers behind its upload path.

Checksum validation, file-size validation, and the two chunking strategies
(`upload_file`'s in-memory buffer vs `upload_file_chunked`'s caller-supplied stream) all
run before, or independently of, any RPC, so they live here as standalone functions
that do not need a connected client to test or use.
"""

from __future__ import annotations

import collections.abc
import hashlib
from typing import AsyncIterable, AsyncIterator, Iterable, Iterator, Optional, TypeVar, Union
from uuid import uuid4

import grpc.aio

from rociadb_sdk._pagination import _optional_cursor, _page_request
from rociadb_sdk._pb.upstream.v1 import upstream_pb2 as pb
from rociadb_sdk._pb.upstream.v1 import upstream_pb2_grpc as rpc
from rociadb_sdk.errors import RociaDbValidationError, _call, _status_error
from rociadb_sdk.types import FileMetadata, Page, RawUploadMessage

#: Fixed size of every upload message except the last - the server's own per-message
#: ceiling, and the only chunk size safe against a server predating 1.0.0-rc.16.
UPLOAD_CHUNK_BYTES = 1_048_576

#: Required exact length, in bytes, of a SHA-256 checksum on the first `Upload` message.
CHECKSUM_LEN = 32

#: Client-side ceiling mirroring the server's default `limits.max_file_bytes`.
MAX_FILE_BYTES = 5 * 1024 * 1024 * 1024


def _validate_file_size(size_bytes: int) -> None:
    """Reject a file whose size exceeds `MAX_FILE_BYTES`, before any network call."""
    if size_bytes > MAX_FILE_BYTES:
        raise RociaDbValidationError(
            f"file is {size_bytes} bytes, which exceeds the server's "
            f"{MAX_FILE_BYTES}-byte (5 GiB) limit"
        )


def _require_checksum_len(checksum: bytes) -> None:
    """Reject a checksum whose length is not exactly `CHECKSUM_LEN`, before any RPC."""
    if len(checksum) != CHECKSUM_LEN:
        raise RociaDbValidationError(
            f"checksum must be exactly {CHECKSUM_LEN} bytes (sha256), got {len(checksum)} bytes"
        )


def _resolve_checksum(checksum: Optional[bytes], data: bytes) -> bytes:
    """Resolve the checksum to send for an in-memory upload.

    Computes the SHA-256 digest of `data` when `checksum` is `None`; otherwise
    validates the caller-supplied one is exactly `CHECKSUM_LEN` bytes and returns it
    unchanged. Pure and network-free, so a bad checksum fails before any RPC - the
    server rejects any other length with `INVALID_ARGUMENT`.
    """
    if checksum is None:
        return hashlib.sha256(data).digest()
    _require_checksum_len(checksum)
    return checksum


def _chunk_upload_requests(
    tenant_id: str,
    bucket: str,
    file_id: str,
    data: bytes,
    content_type: str,
    checksum: bytes,
    request_id: str,
) -> Iterator[pb.UploadRequest]:
    """Lazily build the per-chunk `UploadRequest` sequence for an in-memory buffer.

    Only the first request carries file metadata (`tenant_id`, `bucket`, `file_id`,
    `size_bytes`, `content_type`, `checksum`, `request_id`): the server only reads
    those fields off the first message of the stream, so attaching them to every chunk
    would be wasted work. A zero-byte `data` still yields exactly one message, since a
    message carrying no chunk would otherwise never deliver the metadata at all.
    """
    size_bytes = len(data)
    if size_bytes == 0:
        yield pb.UploadRequest(
            tenant_id=tenant_id,
            bucket=bucket,
            file_id=file_id,
            size_bytes=0,
            content_type=content_type,
            checksum=checksum,
            chunk=b"",
            request_id=request_id,
        )
        return

    for index, start in enumerate(range(0, size_bytes, UPLOAD_CHUNK_BYTES)):
        end = min(start + UPLOAD_CHUNK_BYTES, size_bytes)
        if index == 0:
            yield pb.UploadRequest(
                tenant_id=tenant_id,
                bucket=bucket,
                file_id=file_id,
                size_bytes=size_bytes,
                content_type=content_type,
                checksum=checksum,
                chunk=data[start:end],
                request_id=request_id,
            )
        else:
            yield pb.UploadRequest(chunk=data[start:end])


_X = TypeVar("_X")


async def _iter_any(source: Union[AsyncIterable[_X], Iterable[_X]]) -> AsyncIterator[_X]:
    """Adapt a sync-or-async source of items into a single async iterator.

    Shared by `_rechunk_upload_requests` (byte pieces) and `_FileMixin.upload_file_raw`
    (`RawUploadMessage` items).
    """
    if isinstance(source, collections.abc.AsyncIterable):
        async for item in source:
            yield item
    else:
        for item in source:
            yield item


async def _rechunk_upload_requests(
    tenant_id: str,
    bucket: str,
    file_id: str,
    size_bytes: int,
    content_type: str,
    checksum: bytes,
    request_id: str,
    chunks: Union[AsyncIterable[bytes], Iterable[bytes]],
) -> AsyncIterator[pb.UploadRequest]:
    """Re-chunk an arbitrarily-sized byte source into `UploadRequest` messages of
    exactly `UPLOAD_CHUNK_BYTES` each (the last one possibly shorter), buffering no
    more than one outgoing chunk's worth of bytes at a time.

    Validates the running total against `size_bytes` as it is consumed: a piece that
    would push the total past `size_bytes` raises `RociaDbValidationError` before it is
    turned into a request, and a source left short of `size_bytes` once exhausted
    raises the same error right after its last real chunk - directly out of iteration,
    since an async generator can raise mid-stream without any extra machinery.

    A zero-byte source (`size_bytes == 0`, no chunks at all) still yields exactly one
    empty request, since a message carrying no chunk would otherwise never deliver the
    file metadata to the server.
    """
    buffer = bytearray()
    total_written = 0
    wrote_any = False
    first = True

    def build_request(piece: bytes) -> pb.UploadRequest:
        nonlocal first
        if not first:
            return pb.UploadRequest(chunk=piece)
        first = False
        return pb.UploadRequest(
            tenant_id=tenant_id,
            bucket=bucket,
            file_id=file_id,
            size_bytes=size_bytes,
            content_type=content_type,
            checksum=checksum,
            chunk=piece,
            request_id=request_id,
        )

    def check_total(piece_len: int) -> None:
        if total_written + piece_len > size_bytes:
            raise RociaDbValidationError(
                f"upload_file_chunked received more data than size_bytes "
                f"({size_bytes} bytes) declared"
            )

    async for piece in _iter_any(chunks):
        buffer.extend(piece)
        while len(buffer) >= UPLOAD_CHUNK_BYTES:
            check_total(UPLOAD_CHUNK_BYTES)
            out = bytes(buffer[:UPLOAD_CHUNK_BYTES])
            del buffer[:UPLOAD_CHUNK_BYTES]
            total_written += UPLOAD_CHUNK_BYTES
            wrote_any = True
            yield build_request(out)

    if buffer or not wrote_any:
        check_total(len(buffer))
        total_written += len(buffer)
        yield build_request(bytes(buffer))

    if total_written != size_bytes:
        raise RociaDbValidationError(
            f"upload_file_chunked received {total_written} bytes, short of the "
            f"declared size_bytes ({size_bytes} bytes)"
        )


class _FileMixin:
    """FileService RPC methods: three upload tiers, two download forms, and listings."""

    _files: rpc.FileServiceStub

    async def upload_file(
        self,
        tenant_id: str,
        bucket: str,
        file_id: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        checksum: Optional[bytes] = None,
        request_id: Optional[str] = None,
    ) -> None:
        """Upload an in-memory buffer, auto-chunked into `UPLOAD_CHUNK_BYTES` messages.

        `checksum` is computed automatically from `data` when omitted; a supplied one
        must be exactly `CHECKSUM_LEN` bytes. Rejects a file over `MAX_FILE_BYTES`
        before any network call. Re-uploading an existing `file_id` replaces it without
        error. `request_id` defaults to ``f"upload_file:{uuid4()}"`` when omitted.
        """
        _validate_file_size(len(data))
        resolved_checksum = _resolve_checksum(checksum, data)
        requests = _chunk_upload_requests(
            tenant_id,
            bucket,
            file_id,
            data,
            content_type,
            resolved_checksum,
            request_id if request_id is not None else f"upload_file:{uuid4()}",
        )
        await _call("Upload", self._files.Upload(requests))

    async def upload_file_chunked(
        self,
        tenant_id: str,
        bucket: str,
        file_id: str,
        size_bytes: int,
        checksum: bytes,
        chunks: Union[AsyncIterable[bytes], Iterable[bytes]],
        *,
        content_type: str = "application/octet-stream",
        request_id: Optional[str] = None,
    ) -> None:
        """Upload from a caller-supplied stream, re-chunked into `UPLOAD_CHUNK_BYTES`
        messages without buffering the whole file.

        The caller supplies the exact total `size_bytes` and a precomputed 32-byte
        SHA-256 `checksum` up front: file metadata travels on the first outgoing
        message, before this method has read anything from `chunks`, so unlike
        `upload_file` neither value can be computed for you here. Both are validated
        before any network call; the running total is validated against `size_bytes` as
        `chunks` is consumed, raising `RociaDbValidationError` on overflow or shortfall
        instead of silently sending a file that would fail to reassemble correctly.
        """
        _validate_file_size(size_bytes)
        _require_checksum_len(checksum)
        requests = _rechunk_upload_requests(
            tenant_id,
            bucket,
            file_id,
            size_bytes,
            content_type,
            checksum,
            request_id if request_id is not None else f"upload_file:{uuid4()}",
            chunks,
        )
        await _call("Upload", self._files.Upload(requests))

    async def upload_file_raw(
        self,
        requests: Union[AsyncIterable[RawUploadMessage], Iterable[RawUploadMessage]],
    ) -> None:
        """Raw upload escape hatch: every message is sent exactly as given.

        No re-chunking, no checksum validation, and no first/later-message distinction -
        the caller is fully responsible for reproducing the server's wire contract (see
        `RawUploadMessage`). A `chunk` over 1 MiB, a checksum of the wrong length, or a
        mismatched `size_bytes` all fail with `INVALID_ARGUMENT` rather than corrupting
        anything silently; the one thing the server never verifies is whether `checksum`
        actually matches the bytes sent.
        """

        async def to_proto() -> AsyncIterator[pb.UploadRequest]:
            async for message in _iter_any(requests):
                yield pb.UploadRequest(
                    tenant_id=message.tenant_id,
                    bucket=message.bucket,
                    file_id=message.file_id,
                    size_bytes=message.size_bytes,
                    content_type=message.content_type,
                    checksum=message.checksum,
                    chunk=message.chunk,
                    request_id=message.request_id,
                )

        await _call("Upload", self._files.Upload(to_proto()))

    async def download_file_stream(
        self, tenant_id: str, bucket: str, file_id: str
    ) -> AsyncIterator[bytes]:
        """Download file chunks lazily without buffering the whole file.

        Cancels the underlying call if the consumer stops iterating early (breaks out
        of, or raises out of, the surrounding loop).
        """
        call = self._files.Download(
            pb.DownloadRequest(tenant_id=tenant_id, bucket=bucket, file_id=file_id)
        )
        try:
            async for response in call:
                yield response.chunk
        except grpc.aio.AioRpcError as exc:
            raise _status_error("Download", exc) from exc
        finally:
            call.cancel()

    async def download_file(self, tenant_id: str, bucket: str, file_id: str) -> bytes:
        """Download a complete file into memory. Prefer `download_file_stream` for large files."""
        pieces = [chunk async for chunk in self.download_file_stream(tenant_id, bucket, file_id)]
        return b"".join(pieces)

    async def stat_file(self, tenant_id: str, bucket: str, file_id: str) -> FileMetadata:
        """Fetch size, content type, checksum, and timestamps for one stored file."""
        response = await _call(
            "Stat",
            self._files.Stat(pb.StatRequest(tenant_id=tenant_id, bucket=bucket, file_id=file_id)),
        )
        return FileMetadata(
            size_bytes=response.size_bytes,
            content_type=response.content_type,
            checksum=response.checksum,
            created_at=response.created_at,
            updated_at=response.updated_at,
        )

    async def delete_file(
        self,
        tenant_id: str,
        bucket: str,
        file_id: str,
        *,
        request_id: Optional[str] = None,
    ) -> None:
        """Delete one stored file. `request_id` defaults to ``f"delete_file:{uuid4()}"``."""
        await _call(
            "Delete",
            self._files.Delete(
                pb.DeleteRequest(
                    tenant_id=tenant_id,
                    bucket=bucket,
                    file_id=file_id,
                    request_id=request_id if request_id is not None else f"delete_file:{uuid4()}",
                )
            ),
        )

    async def list_buckets(
        self, tenant_id: str, *, limit: Optional[int] = None, cursor: Optional[str] = None
    ) -> Page[str]:
        """List the bucket names holding at least one file."""
        response = await _call(
            "ListBuckets",
            self._files.ListBuckets(
                pb.ListBucketsRequest(tenant_id=tenant_id, page=_page_request(limit, cursor))
            ),
        )
        return Page(
            items=list(response.buckets),
            next_cursor=_optional_cursor(response.page.next_cursor),
        )

    async def list_files(
        self,
        tenant_id: str,
        bucket: str,
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> Page[str]:
        """List the file ids stored in one bucket."""
        response = await _call(
            "ListFiles",
            self._files.ListFiles(
                pb.ListFilesRequest(
                    tenant_id=tenant_id, bucket=bucket, page=_page_request(limit, cursor)
                )
            ),
        )
        return Page(
            items=list(response.file_ids),
            next_cursor=_optional_cursor(response.page.next_cursor),
        )

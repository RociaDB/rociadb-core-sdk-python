"""One assertion per SDK default, each citing the exact value it must hold.

These constants are the parity contract with the SDK's sibling implementations in
other languages: a silent change to any of them is a functional regression even though
nothing else in the type system would catch it. Behavior already covered elsewhere
(the refresh-interval *formula*, `page limit == 0` rejection, ...) is not repeated here
- this file is only about the literal default values themselves.
"""

from __future__ import annotations

import inspect

from rocia_db_sdk._pagination import DEFAULT_PAGE_SIZE
from rocia_db_sdk.auth import MIN_REFRESH_INTERVAL
from rocia_db_sdk.client import CONCURRENT_REQUESTS, DEFAULT_CONNECT_TIMEOUT, DEFAULT_HOST
from rocia_db_sdk.file import CHECKSUM_LEN, MAX_FILE_BYTES, UPLOAD_CHUNK_BYTES, _FileMixin
from rocia_db_sdk.graph import _NEIGHBOR_HYDRATION_PAGE_SIZE


def test_default_page_size_is_20() -> None:
    assert DEFAULT_PAGE_SIZE == 20


def test_concurrent_requests_is_10() -> None:
    assert CONCURRENT_REQUESTS == 10


def test_default_connect_timeout_is_10_seconds() -> None:
    assert DEFAULT_CONNECT_TIMEOUT == 10.0


def test_upload_chunk_bytes_is_exactly_1_mebibyte() -> None:
    assert UPLOAD_CHUNK_BYTES == 1_048_576


def test_checksum_len_is_32_bytes() -> None:
    assert CHECKSUM_LEN == 32


def test_max_file_bytes_is_5_gibibytes() -> None:
    assert MAX_FILE_BYTES == 5_368_709_120


def test_min_refresh_interval_is_5_seconds() -> None:
    assert MIN_REFRESH_INTERVAL == 5.0


def test_neighbor_hydration_internal_page_size_is_50() -> None:
    assert _NEIGHBOR_HYDRATION_PAGE_SIZE == 50


def test_default_host_is_the_local_loopback_endpoint() -> None:
    assert DEFAULT_HOST == "http://127.0.0.1:50051"


def test_upload_file_default_content_type_is_octet_stream() -> None:
    default = inspect.signature(_FileMixin.upload_file).parameters["content_type"].default
    assert default == "application/octet-stream"


def test_upload_file_chunked_default_content_type_is_octet_stream() -> None:
    default = inspect.signature(_FileMixin.upload_file_chunked).parameters["content_type"].default
    assert default == "application/octet-stream"

"""No-network test doubles standing in for a generated grpc.aio service stub.

Each mixin under test only ever does ``self._some_stub.SomeRpc(request)`` and awaits
(or, for `Download`/`Upload`, iterates) the result, so a plain recording callable is
enough to exercise the mixin's request-building and response-decoding logic without a
channel, a server, or the network.
"""

from __future__ import annotations

from typing import Any, AsyncIterator, List, Optional


class FakeUnaryCall:
    """Stands in for one unary-unary (or unary-stream-shaped-as-unary) stub method.

    Records every request it is called with. Returns the scripted responses in order,
    repeating the last one once exhausted (mirrors `_FakeFetcher` in test_auth.py) -
    or raises the scripted exception instead, if one was set.
    """

    def __init__(self) -> None:
        self.requests: List[Any] = []
        self._responses: List[Any] = []
        self._exception: Optional[BaseException] = None

    def returns(self, *responses: Any) -> FakeUnaryCall:
        self._responses = list(responses)
        return self

    def raises(self, exception: BaseException) -> FakeUnaryCall:
        self._exception = exception
        return self

    async def __call__(self, request: Any) -> Any:
        self.requests.append(request)
        if self._exception is not None:
            raise self._exception
        if not self._responses:
            raise AssertionError("FakeUnaryCall was called with no scripted response")
        index = min(len(self.requests), len(self._responses)) - 1
        return self._responses[index]


class FakeStreamUnaryCall:
    """Stands in for a stream-unary stub method (`FileService.Upload`).

    Fully drains the caller's request iterator (sync or async) before returning,
    exactly as a real call would need to before the server could respond.
    """

    def __init__(self) -> None:
        self.received: List[Any] = []
        self._response: Any = None
        self._exception: Optional[BaseException] = None

    def returns(self, response: Any) -> FakeStreamUnaryCall:
        self._response = response
        return self

    def raises(self, exception: BaseException) -> FakeStreamUnaryCall:
        self._exception = exception
        return self

    async def __call__(self, request_iterator: Any) -> Any:
        if hasattr(request_iterator, "__aiter__"):
            async for item in request_iterator:
                self.received.append(item)
        else:
            for item in request_iterator:
                self.received.append(item)
        if self._exception is not None:
            raise self._exception
        return self._response


class _FakeDownloadResponseStream:
    def __init__(
        self, chunks: List[Any], exception: Optional[BaseException], parent: FakeDownloadStub
    ) -> None:
        self._chunks = chunks
        self._exception = exception
        self._parent = parent

    def __aiter__(self) -> AsyncIterator[Any]:
        return self._generate()

    async def _generate(self) -> AsyncIterator[Any]:
        for chunk in self._chunks:
            yield chunk
        if self._exception is not None:
            raise self._exception

    def cancel(self) -> None:
        self._parent.cancelled = True


class FakeDownloadStub:
    """Stands in for `FileService.Download` (unary-stream): a callable that returns an
    async-iterable-with-`cancel()` call object, matching `grpc.aio`'s shape.
    """

    def __init__(self, chunks: Optional[List[Any]] = None) -> None:
        self._chunks = list(chunks) if chunks is not None else []
        self._exception: Optional[BaseException] = None
        self.requests: List[Any] = []
        self.cancelled = False

    def raises_after_chunks(self, exception: BaseException) -> FakeDownloadStub:
        self._exception = exception
        return self

    def __call__(self, request: Any) -> _FakeDownloadResponseStream:
        self.requests.append(request)
        return _FakeDownloadResponseStream(self._chunks, self._exception, self)

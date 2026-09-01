"""Unit tests for `_run_bounded`, the bounded-concurrency batch runner."""

from __future__ import annotations

import asyncio
from typing import List

import pytest

from rocia_db_sdk.client import _run_bounded


async def test_run_bounded_on_an_empty_input_returns_an_empty_list_without_calling_worker() -> None:
    calls: List[int] = []

    async def worker(item: int) -> int:
        calls.append(item)
        return item

    result = await _run_bounded([], 3, worker)
    assert result == []
    assert calls == []


async def test_run_bounded_preserves_input_order_regardless_of_completion_order() -> None:
    # Item i sleeps for (n - i) ticks, so results would come back in the wrong order
    # if _run_bounded didn't explicitly preserve the caller's ordering.
    async def worker(item: int) -> int:
        await asyncio.sleep((5 - item) * 0.01)
        return item * 10

    result = await _run_bounded([0, 1, 2, 3, 4], 5, worker)
    assert result == [0, 10, 20, 30, 40]


async def test_run_bounded_never_exceeds_the_requested_concurrency() -> None:
    in_flight = 0
    max_in_flight = 0
    lock = asyncio.Lock()

    async def worker(item: int) -> int:
        nonlocal in_flight, max_in_flight
        async with lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.01)
        async with lock:
            in_flight -= 1
        return item

    await _run_bounded(list(range(10)), 3, worker)
    assert max_in_flight <= 3


async def test_run_bounded_raises_the_first_failure_and_cancels_pending_work() -> None:
    started: List[int] = []
    completed: List[int] = []

    async def worker(item: int) -> int:
        started.append(item)
        if item == 2:
            raise ValueError("boom")
        await asyncio.sleep(0.05)
        completed.append(item)
        return item

    with pytest.raises(ValueError, match="boom"):
        await _run_bounded(list(range(6)), 6, worker)

    # The failing item's siblings were cancelled mid-sleep, so none of them reached
    # the point of recording completion.
    assert completed == []


async def test_run_bounded_never_starts_work_still_queued_behind_the_concurrency_limit() -> None:
    # Concurrency 2 over 6 items, where the very first item fails without ever
    # yielding control. Every task is scheduled up front, so - before `_run_bounded`
    # even gets a chance to notice the failure and cancel anything - each task runs its
    # first step in order: items 0 and 1 fill the two concurrency slots: item 0 fails
    # and (via its own `async with semaphore` unwinding) hands its slot straight back,
    # letting item 2 also start before the failure is ever observed. That is the most
    # work an immediate failure can ever cause to start: items 3-5, still waiting on
    # the semaphore when cancellation runs, must never call `worker` at all.
    started: List[int] = []

    async def worker(item: int) -> int:
        started.append(item)
        if item == 0:
            raise ValueError("boom")
        await asyncio.sleep(0.05)
        return item

    with pytest.raises(ValueError, match="boom"):
        await _run_bounded(list(range(6)), 2, worker)

    assert started == [0, 1, 2]


async def test_run_bounded_is_safe_to_replay_after_a_failure() -> None:
    async def failing_worker(item: int) -> int:
        raise ValueError("boom")

    with pytest.raises(ValueError):
        await _run_bounded([1, 2, 3], 2, failing_worker)

    async def succeeding_worker(item: int) -> int:
        return item

    result = await _run_bounded([1, 2, 3], 2, succeeding_worker)
    assert result == [1, 2, 3]

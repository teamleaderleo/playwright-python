# Copyright (c) Microsoft Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
from typing import Awaitable, Callable

import pytest

from playwright.async_api import async_playwright


async def test_cancelled_stop_can_be_retried() -> None:
    manager = async_playwright()
    playwright = await manager.start()

    transport = manager._connection._transport
    original_wait: Callable[[], Awaitable[None]] = transport.wait_until_stopped
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked_wait_until_stopped() -> None:
        entered.set()
        await release.wait()
        await original_wait()

    transport.wait_until_stopped = blocked_wait_until_stopped  # type: ignore[method-assign]

    stop_task = asyncio.create_task(playwright.stop())
    await entered.wait()
    stop_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stop_task

    release.set()
    await original_wait()

    await playwright.stop()
    assert manager._connection._closed_error is not None


async def test_concurrent_stop_callers_share_one_operation() -> None:
    manager = async_playwright()
    playwright = await manager.start()

    connection = manager._connection
    original_stop: Callable[[], Awaitable[None]] = connection.stop_async
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def controlled_stop() -> None:
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        await original_stop()

    connection.stop_async = controlled_stop  # type: ignore[method-assign]

    first = asyncio.create_task(playwright.stop())
    await entered.wait()
    second = asyncio.create_task(playwright.stop())
    await asyncio.sleep(0)

    assert calls == 1
    assert manager._stop_task is not None

    release.set()
    await asyncio.gather(first, second)
    assert connection._closed_error is not None


async def test_repeated_successful_stop_reuses_completion() -> None:
    manager = async_playwright()
    playwright = await manager.start()

    await playwright.stop()
    stop_task = manager._stop_task

    await playwright.stop()

    assert manager._stop_task is stop_task
    assert manager._connection._closed_error is not None


async def test_stop_failure_is_shared_with_later_callers() -> None:
    manager = async_playwright()
    playwright = await manager.start()

    connection = manager._connection
    original_stop: Callable[[], Awaitable[None]] = connection.stop_async
    failure = RuntimeError("stop failed")
    calls = 0

    async def failing_stop() -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        raise failure

    connection.stop_async = failing_stop  # type: ignore[method-assign]

    try:
        first = asyncio.create_task(playwright.stop())
        second = asyncio.create_task(playwright.stop())
        outcomes = await asyncio.gather(first, second, return_exceptions=True)

        assert calls == 1
        assert outcomes == [failure, failure]

        with pytest.raises(RuntimeError) as error:
            await playwright.stop()
        assert error.value is failure
        assert calls == 1
    finally:
        connection.stop_async = original_stop  # type: ignore[method-assign]
        await original_stop()

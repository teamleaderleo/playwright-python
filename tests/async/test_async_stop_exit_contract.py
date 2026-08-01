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
from typing import Any, Awaitable, Callable

import pytest

from playwright.async_api import async_playwright


async def test_async_with_body_error_waits_for_successful_stop() -> None:
    manager = async_playwright()
    body_error = RuntimeError("body failed")
    stop_entered = asyncio.Event()
    release_stop = asyncio.Event()

    async def run() -> None:
        async with manager:
            connection = manager._connection
            original_stop: Callable[[], Awaitable[None]] = connection.stop_async

            async def controlled_stop() -> None:
                stop_entered.set()
                await release_stop.wait()
                await original_stop()

            connection.stop_async = controlled_stop  # type: ignore[method-assign]
            raise body_error

    task = asyncio.create_task(run())
    await stop_entered.wait()
    assert not task.done()

    release_stop.set()
    with pytest.raises(RuntimeError) as error:
        await task
    assert error.value is body_error
    assert manager._connection._closed_error is not None


async def test_async_with_body_cancellation_waits_for_stop() -> None:
    manager = async_playwright()
    body_entered = asyncio.Event()
    stop_entered = asyncio.Event()
    release_stop = asyncio.Event()

    async def run() -> None:
        async with manager:
            connection = manager._connection
            original_stop: Callable[[], Awaitable[None]] = connection.stop_async

            async def controlled_stop() -> None:
                stop_entered.set()
                await release_stop.wait()
                await original_stop()

            connection.stop_async = controlled_stop  # type: ignore[method-assign]
            body_entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(run())
    await body_entered.wait()
    task.cancel()
    await stop_entered.wait()
    assert not task.done()

    release_stop.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert manager._connection._closed_error is not None


async def test_cleanup_failure_takes_precedence_and_chains_body_error() -> None:
    manager = async_playwright()
    body_error = RuntimeError("body failed")
    cleanup_error = RuntimeError("cleanup failed")

    async def run() -> None:
        async with manager:
            connection = manager._connection

            async def failing_stop() -> None:
                raise cleanup_error

            connection.stop_async = failing_stop  # type: ignore[method-assign]
            raise body_error

    try:
        with pytest.raises(RuntimeError) as error:
            await run()
        assert error.value is cleanup_error
        assert error.value.__context__ is body_error
    finally:
        manager._connection._transport.request_stop()
        await manager._connection._transport.wait_until_stopped()
        manager._connection.cleanup()


async def test_outer_cancellation_before_shared_stop_first_timeslice() -> None:
    manager = async_playwright()
    playwright = await manager.start()

    connection = manager._connection
    original_stop: Callable[[], Awaitable[None]] = connection.stop_async
    stop_started = asyncio.Event()
    release_stop = asyncio.Event()
    calls = 0

    async def controlled_stop() -> None:
        nonlocal calls
        calls += 1
        stop_started.set()
        await release_stop.wait()
        await original_stop()

    connection.stop_async = controlled_stop  # type: ignore[method-assign]

    first = asyncio.create_task(playwright.stop())
    asyncio.get_running_loop().call_soon(first.cancel)
    with pytest.raises(asyncio.CancelledError):
        await first

    assert manager._stop_task is not None
    assert not manager._stop_task.cancelled()
    await stop_started.wait()

    release_stop.set()
    await playwright.stop()
    assert calls == 1
    assert connection._closed_error is not None


async def test_unjoined_stop_failure_reaches_loop_exception_handler() -> None:
    manager = async_playwright()
    playwright = await manager.start()

    connection = manager._connection
    original_stop: Callable[[], Awaitable[None]] = connection.stop_async
    entered = asyncio.Event()
    release = asyncio.Event()
    completed = asyncio.Event()
    failure = RuntimeError("unjoined stop failed")
    contexts: list[dict[str, Any]] = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()

    async def failing_stop() -> None:
        entered.set()
        await release.wait()
        raise failure

    connection.stop_async = failing_stop  # type: ignore[method-assign]
    loop.set_exception_handler(lambda _loop, context: contexts.append(context))

    try:
        waiter = asyncio.create_task(playwright.stop())
        await entered.wait()
        assert manager._stop_task is not None
        manager._stop_task.add_done_callback(lambda _task: completed.set())

        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        release.set()
        await completed.wait()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert len(contexts) == 1
        assert contexts[0]["exception"] is failure
        assert contexts[0]["message"] == "Playwright stop task failed"

        with pytest.raises(RuntimeError) as error:
            await playwright.stop()
        assert error.value is failure
        assert len(contexts) == 1
    finally:
        loop.set_exception_handler(previous_handler)
        connection.stop_async = original_stop
        await original_stop()

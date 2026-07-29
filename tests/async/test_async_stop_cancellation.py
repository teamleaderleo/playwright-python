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

    try:
        await playwright.stop()
        assert manager._connection._closed_error is not None
    finally:
        if manager._connection._closed_error is None:
            manager._connection.cleanup()

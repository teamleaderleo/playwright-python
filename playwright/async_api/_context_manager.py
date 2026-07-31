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
from typing import Any, Optional

from playwright._impl._connection import Connection
from playwright._impl._object_factory import create_remote_object
from playwright._impl._transport import PipeTransport
from playwright.async_api._generated import Playwright as AsyncPlaywright


class PlaywrightContextManager:
    def __init__(self) -> None:
        self._connection: Connection
        self._stop_task: Optional[asyncio.Task[None]] = None
        self._stop_waiters = 0
        self._stop_failure: Optional[BaseException] = None
        self._stop_failure_observed = False
        self._stop_failure_reported = False
        self._stop_failure_report_handle: Optional[asyncio.Handle] = None

    async def __aenter__(self) -> AsyncPlaywright:
        loop = asyncio.get_running_loop()
        self._connection = Connection(
            None,
            create_remote_object,
            PipeTransport(loop),
            loop,
        )
        loop.create_task(self._connection.run())
        playwright_future = self._connection.playwright_future

        done, _ = await asyncio.wait(
            {self._connection._transport.on_error_future, playwright_future},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not playwright_future.done():
            playwright_future.cancel()
        playwright = AsyncPlaywright(next(iter(done)).result())
        playwright.stop = self.__aexit__  # type: ignore
        return playwright

    async def start(self) -> AsyncPlaywright:
        return await self.__aenter__()

    def _stop_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        failure = task.exception()
        if failure is None:
            return
        self._stop_failure = failure
        self._schedule_unobserved_stop_failure()

    def _cancel_unobserved_stop_failure_report(self) -> None:
        if self._stop_failure_report_handle is None:
            return
        self._stop_failure_report_handle.cancel()
        self._stop_failure_report_handle = None

    def _schedule_unobserved_stop_failure(self) -> None:
        if (
            self._stop_failure is None
            or self._stop_failure_observed
            or self._stop_failure_reported
            or self._stop_waiters != 0
            or self._stop_failure_report_handle is not None
        ):
            return
        assert self._stop_task is not None
        self._stop_failure_report_handle = self._stop_task.get_loop().call_soon(
            self._report_unobserved_stop_failure
        )

    def _report_unobserved_stop_failure(self) -> None:
        self._stop_failure_report_handle = None
        if (
            self._stop_failure is None
            or self._stop_failure_observed
            or self._stop_failure_reported
            or self._stop_waiters != 0
        ):
            return
        assert self._stop_task is not None
        self._stop_failure_reported = True
        self._stop_task.get_loop().call_exception_handler(
            {
                "message": "Playwright stop task failed",
                "exception": self._stop_failure,
                "task": self._stop_task,
            }
        )

    async def __aexit__(self, *args: Any) -> None:
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._connection.stop_async())
            self._stop_task.add_done_callback(self._stop_done)

        self._cancel_unobserved_stop_failure_report()
        self._stop_waiters += 1
        try:
            await asyncio.shield(self._stop_task)
        except asyncio.CancelledError:
            raise
        except BaseException:
            self._stop_failure_observed = True
            self._cancel_unobserved_stop_failure_report()
            raise
        finally:
            self._stop_waiters -= 1
            self._schedule_unobserved_stop_failure()

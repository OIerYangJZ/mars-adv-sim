"""Periodic failure detector for the registry simulation."""

from __future__ import annotations

import asyncio
import contextlib

from .registry import Registry


class Watchdog:
    def __init__(self, registry: Registry, scan_interval: float = 1.0) -> None:
        self.registry = registry
        self.scan_interval = scan_interval
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="registry-watchdog")

    async def _run(self) -> None:
        try:
            while True:
                await self.registry.sweep_expired()
                await asyncio.sleep(self.scan_interval)
        except asyncio.CancelledError:
            raise

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

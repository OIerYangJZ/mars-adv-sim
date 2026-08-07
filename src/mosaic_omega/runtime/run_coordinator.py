"""CLI entrypoint for the routing service."""

from __future__ import annotations

import argparse
import asyncio

from .coordinator import Coordinator
from .settings import add_transport_arguments, transport_settings_from_args
from .transport import create_transport


async def run(args: argparse.Namespace) -> None:
    settings = transport_settings_from_args(args)
    transport = create_transport(settings, "coordinator", Coordinator.subscriptions())
    placement_port = None
    if args.resource_scheduler:
        from ..scheduler.placement_adapter import ResourceSchedulerPlacementAdapter
        placement_port = ResourceSchedulerPlacementAdapter()
    coordinator = Coordinator(
        transport,
        watchdog_interval=args.watchdog_interval,
        resync_grace_s=args.resync_grace_s,
        placement_port=placement_port,
    )
    await coordinator.start()
    try:
        await asyncio.Event().wait()
    finally:
        await coordinator.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="动态路由协调服务")
    add_transport_arguments(parser)
    parser.add_argument("--watchdog-interval", type=float, default=1.0)
    parser.add_argument(
        "--resource-scheduler",
        action="store_true",
        help="enable the bundled device/edge/cloud scheduling adapter",
    )
    parser.add_argument(
        "--resync-grace-s",
        type=float,
        default=8.0,
        help="seconds to wait for agent registration/state sync after MQTT recovery",
    )
    asyncio.run(run(parser.parse_args()))

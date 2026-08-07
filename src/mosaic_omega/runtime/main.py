"""Run a four-agent dynamic-registration demonstration."""

from __future__ import annotations

import argparse
import asyncio

from .agent_sim import AgentSimulator
from .models import AgentProfile
from .registry import Registry
from .watchdog import Watchdog


def profiles() -> list[AgentProfile]:
    return [
        AgentProfile("searcher-01", "检索智能体", ("search",), "sim://searcher-01"),
        AgentProfile("coder-01", "编程智能体", ("code",), "sim://coder-01"),
        AgentProfile("planner-01", "规划智能体", ("plan",), "sim://planner-01"),
        AgentProfile("analyst-01", "分析智能体", ("analysis",), "sim://analyst-01"),
    ]


async def show_state(registry: Registry, label: str) -> None:
    snapshot = await registry.topology_snapshot()
    online = await registry.online_agents()
    print(f"\n[{label}] topology=v{snapshot.version}")
    print("online:", ", ".join(sorted(r.profile.agent_id for r in online)) or "(none)")
    print("nodes :", ", ".join(snapshot.nodes))
    print("edges :", ", ".join(f"{a}->{b}" for a, b in snapshot.edges) or "(none)")


async def run(fast: bool) -> None:
    heartbeat_interval = 0.5 if fast else 3.0
    suspect_after = 1.5 if fast else 6.0
    timeout = 3.0 if fast else 9.0
    scan_interval = 0.2 if fast else 1.0
    registry = Registry(heartbeat_timeout=timeout, suspect_after=suspect_after)
    watchdog = Watchdog(registry, scan_interval=scan_interval)
    agents = [AgentSimulator(profile, registry, heartbeat_interval) for profile in profiles()]

    await watchdog.start()
    for agent in agents:
        await agent.start()
    await asyncio.sleep(heartbeat_interval * 1.2)
    await show_state(registry, "全部上线")

    coder = next(agent for agent in agents if agent.profile.agent_id == "coder-01")
    print("\n注入故障：coder-01 静默崩溃（停止心跳，不主动注销）")
    await coder.crash()
    await asyncio.sleep(timeout + scan_interval * 2)
    await show_state(registry, "故障检测后")

    print("\n恢复节点：coder-01 使用新 session_id 重新注册")
    await coder.recover()
    await asyncio.sleep(heartbeat_interval * 1.2)
    await show_state(registry, "恢复后")

    print("\n事件日志：")
    events = await registry.events()
    first_timestamp = events[0].timestamp
    for event in events:
        print(f"+{event.timestamp - first_timestamp:05.2f}s {event.event_type:<25} {event.agent_id:<12} topo=v{event.topology_version} {event.detail}")

    for agent in agents:
        await agent.shutdown()
    await watchdog.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="动态注册与心跳检测仿真")
    parser.add_argument("--fast", action="store_true", help="使用约 5 秒完成的快速演示参数")
    args = parser.parse_args()
    asyncio.run(run(args.fast))

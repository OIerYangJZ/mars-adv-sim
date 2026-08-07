from __future__ import annotations

import time

from mosaic_omega.runtime.edge_cloud import PlacementConstraints, PlacementRequest
from mosaic_omega.runtime.models import AgentProfile, AgentRecord, AgentStatus
from mosaic_omega.scheduler.placement_adapter import ResourceSchedulerPlacementAdapter


async def _select():
    now = time.monotonic()
    records = [
        AgentRecord(
            profile=AgentProfile(
                "edge-coder", "edge", ("code",), "",
                tier="edge", resources={"cpu_cores": 8, "memory_mb": 8192}, reliability=0.96,
            ),
            session_id="1", status=AgentStatus.ONLINE, current_load=0,
            joined_at=now, last_heartbeat=now, updated_at=now,
        ),
        AgentRecord(
            profile=AgentProfile(
                "cloud-coder", "cloud", ("code",), "",
                tier="cloud", resources={"cpu_cores": 16, "memory_mb": 16384}, reliability=0.99,
            ),
            session_id="2", status=AgentStatus.ONLINE, current_load=0,
            joined_at=now, last_heartbeat=now, updated_at=now,
        ),
    ]
    request = PlacementRequest(
        task_id="patch",
        required_skills=frozenset({"code"}),
        constraints=PlacementConstraints.from_dict({
            "allowed_tiers": ["device", "edge"],
            "require_local_data": True,
        }),
        metadata={"description": "patch locally", "priority": 8},
    )
    return await ResourceSchedulerPlacementAdapter().select(request, records)


def test_hard_placement_filter_blocks_cloud() -> None:
    import asyncio
    result = asyncio.run(_select())
    assert result is not None
    assert result.agent_id == "edge-coder"
    assert result.tier.value == "edge"

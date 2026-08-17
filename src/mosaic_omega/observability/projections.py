"""Pure dashboard projections over authoritative MOSAIC-Ω runtime data."""
from __future__ import annotations

import time
from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping, Sequence

from .tracing import trace_summary


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "to_dict"):
        return dict(value.to_dict())
    return {}


def _status(task: Mapping[str, Any]) -> str:
    return str(task.get("status", task.get("state", "UNKNOWN")))


def _event_type(event: Mapping[str, Any]) -> str:
    return str(event.get("type", event.get("event_type", "UNKNOWN")))


def _task_id(task: Mapping[str, Any]) -> str:
    return str(task.get("node_id", task.get("task_id", "")))


def _event_time(event: Mapping[str, Any]) -> float:
    value = event.get("timestamp", event.get("occurred_at", 0.0))
    return float(value or 0.0)


def _recovery_events(events: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    tokens = ("RECOVERY", "RECOVER", "REPLAN", "ROLLBACK", "SAFE_STOP", "ERROR", "FAILED")
    return [event for event in events if any(token in _event_type(event).upper() for token in tokens)]


def _communication_summary(messages: Sequence[dict[str, Any]], telemetry: Mapping[str, Any]) -> dict[str, Any]:
    action_counts = Counter(str(item.get("policy_action", "UNKNOWN")).upper() for item in messages)
    input_tokens = telemetry.get("model_input_tokens", {}) if isinstance(telemetry, Mapping) else {}
    output_tokens = telemetry.get("model_output_tokens", {}) if isinstance(telemetry, Mapping) else {}
    measured_count = int(input_tokens.get("count", 0) or 0) + int(output_tokens.get("count", 0) or 0)
    token_usage = {
        "status": "measured" if measured_count else "insufficient_data",
        "input": input_tokens,
        "output": output_tokens,
        "note": None if measured_count else "当前运行未提供真实 tokenizer/model usage，控制台不会用字符数伪造 Token。",
    }
    return {
        "total": len(messages),
        "action_counts": dict(action_counts),
        "items": list(messages),
        "queue_wait_ms": telemetry.get("queue_wait_ms", {}) if isinstance(telemetry, Mapping) else {},
        "mqtt_rtt_ms": telemetry.get("mqtt_rtt_ms", {}) if isinstance(telemetry, Mapping) else {},
        "token_usage": token_usage,
    }


def _memory_summary(memory_records: Sequence[dict[str, Any]], context_packs: Mapping[str, Any], metrics: Mapping[str, Any]) -> dict[str, Any]:
    type_counts = Counter(str(item.get("memory_type", "unknown")) for item in memory_records)
    status_counts = Counter(str(item.get("verification_status", "unknown")) for item in memory_records)
    packs = list(context_packs.values())
    # Working memory is an intentionally ephemeral store, not a persisted
    # MemoryRecord stream.  Active ContextPacks are its observable projection.
    type_counts["working"] = max(type_counts.get("working", 0), len(packs))
    total_pack_tokens = sum(int(pack.get("token_estimate", 0) or 0) for pack in packs if isinstance(pack, Mapping))
    total_history_tokens = sum(int(pack.get("full_history_token_estimate", 0) or 0) for pack in packs if isinstance(pack, Mapping))
    return {
        "record_count": len(memory_records),
        "type_counts": dict(type_counts),
        "status_counts": dict(status_counts),
        "metrics": dict(metrics),
        "context_pack_count": len(packs),
        "context_pack_tokens": total_pack_tokens,
        "full_history_tokens": total_history_tokens,
        "token_reduction": (1 - total_pack_tokens / total_history_tokens) if total_history_tokens else None,
        "context_packs": dict(context_packs),
    }


def build_dashboard_snapshot(
    *,
    run_id: str,
    phase: str,
    tasks: Iterable[Any],
    events: Iterable[Any],
    capabilities: Iterable[Any],
    communication: Sequence[Mapping[str, Any]],
    communication_decisions: Sequence[Mapping[str, Any]],
    context_packs: Mapping[str, Mapping[str, Any]],
    memory_records: Iterable[Any],
    memory_metrics: Mapping[str, Any],
    topology_snapshot: Mapping[str, Any],
    topology_telemetry: Mapping[str, Any],
    metric_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    task_dicts = [_as_dict(task) for task in tasks]
    event_dicts = [_as_dict(event) for event in events]
    capability_dicts = [_as_dict(item) for item in capabilities]
    memory_dicts = [_as_dict(item) for item in memory_records]
    event_dicts.sort(key=_event_time)

    status_counts = Counter(_status(task) for task in task_dicts)
    terminal = {"SUCCEEDED", "FAILED", "PAUSED"}
    succeeded = status_counts.get("SUCCEEDED", 0)
    timestamps = [_event_time(event) for event in event_dicts if _event_time(event) > 0]
    duration_ms = round((max(timestamps) - min(timestamps)) * 1000, 6) if len(timestamps) >= 2 else 0.0
    all_terminal = bool(task_dicts) and all(_status(task) in terminal for task in task_dicts)
    if task_dicts and succeeded == len(task_dicts):
        run_status = "SUCCEEDED"
    elif status_counts.get("FAILED", 0):
        run_status = "FAILED"
    elif status_counts.get("PAUSED", 0) and all_terminal:
        run_status = "PAUSED"
    elif event_dicts:
        run_status = "RUNNING"
    else:
        run_status = "CREATED"

    graph_nodes: list[dict[str, Any]] = []
    graph_edges: list[dict[str, str]] = []
    assignments: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    for task in task_dicts:
        node_id = _task_id(task)
        assignment = task.get("assignment") if isinstance(task.get("assignment"), Mapping) else None
        graph_nodes.append({
            "id": node_id,
            "label": task.get("description") or node_id,
            "type": task.get("type", task.get("task_type", "general")),
            "status": _status(task),
            "risk": task.get("risk", "normal"),
            "priority": task.get("priority", 5),
            "assignment": dict(assignment) if assignment else None,
            "acceptance": task.get("acceptance", task.get("acceptance_conditions", [])),
            "evidence_count": len(task.get("evidence", []) or []),
            "attempt": task.get("attempt", 0),
        })
        for parent in task.get("predecessors", task.get("depends_on", [])) or []:
            graph_edges.append({"source": str(parent), "target": node_id})
        if assignment:
            assignments.append({
                "task_id": node_id,
                "task_type": task.get("type", task.get("task_type")),
                "risk": task.get("risk", "normal"),
                **dict(assignment),
            })
        for item in task.get("evidence", []) or []:
            if isinstance(item, Mapping):
                evidence.append(dict(item))

    verification_events: list[dict[str, Any]] = []
    for event in event_dicts:
        if _event_type(event) != "TASK_VERIFIED":
            continue
        payload = event.get("payload", {})
        verification = payload.get("verification", {}) if isinstance(payload, Mapping) else {}
        if isinstance(verification, Mapping):
            verification_events.append(dict(verification) | {
                "event_id": event.get("event_id"),
                "trace_id": event.get("trace_id"),
                "task_id": event.get("node_id", event.get("task_id")),
                "timestamp": event.get("timestamp", event.get("occurred_at")),
            })

    recovery_events = _recovery_events(event_dicts)
    affected_nodes: set[str] = set()
    for event in recovery_events:
        payload = event.get("payload", {})
        if isinstance(payload, Mapping):
            for value in payload.get("affected_task_ids", []) or []:
                affected_nodes.add(str(value))

    topology = dict(topology_snapshot)
    topology.setdefault("nodes", [])
    topology.setdefault("edges", [])
    topology["telemetry"] = dict(topology_telemetry)

    active_agents = [item for item in capability_dicts if item.get("kind") == "agent" and item.get("online", True)]
    scheduler_policy_counts = Counter(str(item.get("policy", "unknown")) for item in assignments)
    cost_values = [float(item.get("total_cost", 0.0) or 0.0) for item in assignments]

    snapshot = {
        "schema_version": "mosaic-console-v1",
        "generated_at": time.time(),
        "phase": phase,
        "run": {
            "run_id": run_id,
            "status": run_status,
            "task_count": len(task_dicts),
            "succeeded": succeeded,
            "failed": status_counts.get("FAILED", 0),
            "running": status_counts.get("RUNNING", 0) + status_counts.get("VERIFYING", 0),
            "ready": status_counts.get("READY", 0),
            "paused": status_counts.get("PAUSED", 0),
            "all_terminal": all_terminal,
            "success_rate": succeeded / len(task_dicts) if task_dicts else 0.0,
            "e2e_latency_ms": duration_ms,
            "event_count": len(event_dicts),
            "evidence_count": len(evidence),
            "message_count": len(communication_decisions or communication),
            "active_agent_count": len(active_agents),
        },
        "task_graph": {"nodes": graph_nodes, "edges": graph_edges, "status_counts": dict(status_counts)},
        "tasks": task_dicts,
        "topology": topology,
        "communication": _communication_summary(
            [dict(item) for item in (communication_decisions or communication)], topology_telemetry
        ),
        "scheduler": {
            "assignments": assignments,
            "policy_counts": dict(scheduler_policy_counts),
            "assignment_count": len(assignments),
            "average_cost": round(sum(cost_values) / len(cost_values), 6) if cost_values else 0.0,
            "capabilities": capability_dicts,
        },
        "memory": _memory_summary(memory_dicts, context_packs, memory_metrics),
        "recovery": {
            "event_count": len(recovery_events),
            "affected_nodes": sorted(affected_nodes),
            "timeline": recovery_events,
        },
        "evidence": {
            "items": evidence,
            "verification_results": verification_events,
            "verified_count": sum(1 for item in evidence if item.get("verification_status") == "VERIFIED"),
            "rejected_count": sum(1 for item in evidence if item.get("verification_status") == "REJECTED"),
        },
        "events": event_dicts,
        "traces": trace_summary(event_dicts),
        "metrics": dict(metric_snapshot),
    }
    return snapshot

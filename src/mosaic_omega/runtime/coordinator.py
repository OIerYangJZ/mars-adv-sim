"""Control-plane service: registry, task DAG, router, topology, and dispatch."""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any

from .dedup import MessageDeduplicator
from .edge_cloud import EdgeCloudSchedulingPort, PlacementDecision, PlacementRequest
from .models import AgentProfile
from .low_entropy import (
    CausalLedger, DecisionImpactEstimator, LowEntropyMetrics, LowEntropyOutbox,
    PolicyAction, ReceiverConditionedCompressor, ReceiverKnowledgeStore,
    semantic_fingerprint,
)
from .protocol import compact_heartbeat_fields, envelope, payload_of
from .registry import Registry
from .routing import DynamicRouter, RouteDecision
from .task_context import TaskContext, TaskContextStore
from .task_messages import TaskMessage
from .tasks import TaskRecord, TaskSpec, TaskStatus, TaskStore
from .transport import MessageTransport

REGISTER_TOPIC = "control/register"
HEARTBEAT_TOPIC = "control/heartbeat"
TASK_NEW_TOPIC = "control/task/new"
RESULT_TOPIC = "control/task/result"
ERROR_TOPIC = "control/task/error"
STATE_SYNC_TOPIC = "control/state_sync"
STATE_SYNC_REQUEST_TOPIC = "control/state_sync/request"
TASK_CONTEXT_UPDATE_TOPIC = "control/task/context/update"
TASK_CONTEXT_SNAPSHOT_REQUEST_TOPIC = "control/task/context/snapshot/request"
CONTEXT_ACK_TOPIC = "control/task/context/ack"
CONTEXT_FEEDBACK_TOPIC = "control/task/context/feedback"


def agent_inbox(agent_id: str) -> str:
    return f"agent/{agent_id}/inbox"


def context_inbox(agent_id: str) -> str:
    """Direct low-entropy deltas; the MQTT topic supplies the message category."""
    return f"agent/{agent_id}/context"


def context_snapshot_inbox(agent_id: str) -> str:
    """Direct complete context replacement after restart or cache loss."""
    return f"agent/{agent_id}/context/snapshot"


def topology_topic(agent_id: str) -> str:
    return f"control/topology/{agent_id}"


class Coordinator:
    """A single coordinator process; MQTT remains only the transport layer."""

    def __init__(
        self,
        transport: MessageTransport,
        registry: Registry | None = None,
        task_store: TaskStore | None = None,
        watchdog_interval: float = 1.0,
        resync_grace_s: float = 8.0,
        placement_port: EdgeCloudSchedulingPort | None = None,
        task_context_store: TaskContextStore | None = None,
    ) -> None:
        self.transport = transport
        self.registry = registry or Registry()
        self.tasks = task_store or TaskStore()
        self.task_contexts = task_context_store or TaskContextStore()
        self.router = DynamicRouter(self.registry)
        self.watchdog_interval = watchdog_interval
        self.resync_grace_s = resync_grace_s
        self.placement_port = placement_port
        self._lock = asyncio.Lock()
        self._watchdog_task: asyncio.Task[None] | None = None
        self._deduplicator = MessageDeduplicator()
        self._broker_connected = False
        self._suspend_expiry_until = 0.0
        self._agent_active_tasks: dict[str, set[str]] = {}
        self._compact_heartbeat_bindings: dict[int, tuple[str, str, int, int]] = {}
        self.receiver_knowledge = ReceiverKnowledgeStore()
        self.context_compressor = ReceiverConditionedCompressor()
        self.impact_estimator = DecisionImpactEstimator()
        self.context_outbox = LowEntropyOutbox()
        self.causal_ledger = CausalLedger()
        self.low_entropy_stats = LowEntropyMetrics()
        self._context_inflight: dict[str, tuple[str, TaskMessage, str, float]] = {}
        self._context_inflight_hashes: set[tuple[str, str]] = set()
        self._low_entropy_task: asyncio.Task[None] | None = None

    @staticmethod
    def subscriptions() -> tuple[str, ...]:
        return (
            REGISTER_TOPIC,
            HEARTBEAT_TOPIC,
            TASK_NEW_TOPIC,
            RESULT_TOPIC,
            ERROR_TOPIC,
            STATE_SYNC_TOPIC,
            TASK_CONTEXT_UPDATE_TOPIC,
            TASK_CONTEXT_SNAPSHOT_REQUEST_TOPIC,
            CONTEXT_ACK_TOPIC,
            CONTEXT_FEEDBACK_TOPIC,
        )

    async def start(self) -> None:
        self.transport.add_connection_listener(self._on_transport_connection)
        await self.transport.start(self.on_message)
        self._watchdog_task = asyncio.create_task(self._watchdog_loop(), name="coordinator-watchdog")
        self._low_entropy_task = asyncio.create_task(
            self._low_entropy_flush_loop(), name="coordinator-low-entropy-outbox"
        )
        print("[coordinator] started")

    async def stop(self) -> None:
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._watchdog_task
        if self._low_entropy_task is not None:
            self._low_entropy_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._low_entropy_task
        await self.transport.stop()

    async def on_message(self, topic: str, message: dict[str, Any]) -> None:
        try:
            async with self._lock:
                compact = compact_heartbeat_fields(message)
                if topic == HEARTBEAT_TOPIC and compact is not None:
                    await self._handle_compact_heartbeat(*compact)
                    return
                logical_message_id = message.get("id", message.get("message_id"))
                if self._deduplicator.is_duplicate(logical_message_id):
                    # Direct low-entropy results use TaskMessage.message_id;
                    # task completion itself remains idempotent in TaskStore.
                    if topic == RESULT_TOPIC:
                        await self._handle_result(message)
                        return
                    print(f"[coordinator] ignored duplicate {message.get('type', 'message')} on {topic}")
                    return
                if topic == REGISTER_TOPIC:
                    await self._handle_register(message)
                elif topic == HEARTBEAT_TOPIC:
                    await self._handle_heartbeat(message)
                elif topic == STATE_SYNC_TOPIC:
                    await self._handle_state_sync(message)
                elif topic == TASK_NEW_TOPIC:
                    await self._handle_new_tasks(message)
                elif topic == TASK_CONTEXT_UPDATE_TOPIC:
                    await self._handle_task_context_update(message)
                elif topic == TASK_CONTEXT_SNAPSHOT_REQUEST_TOPIC:
                    await self._handle_task_context_snapshot_request(message)
                elif topic == CONTEXT_ACK_TOPIC:
                    await self._handle_context_ack(message)
                elif topic == CONTEXT_FEEDBACK_TOPIC:
                    await self._handle_context_feedback(message)
                elif topic == RESULT_TOPIC:
                    await self._handle_result(message)
                elif topic == ERROR_TOPIC:
                    print(f"[coordinator] agent error: {message}")
        except (KeyError, TypeError, ValueError) as exc:
            print(f"[coordinator] rejected message on {topic}: {exc}")

    async def _handle_register(self, message: dict[str, Any]) -> None:
        payload = payload_of(message)
        profile = AgentProfile.from_dict(payload["profile"])
        await self.registry.register(profile, payload["session_id"])
        # A fresh registration may represent a restarted process with an empty
        # cache. Forget optimistic receiver knowledge and rebuild it from ACKs.
        self.receiver_knowledge.reset_agent(profile.agent_id)
        agent_code = payload.get("heartbeat_agent_code")
        session_epoch = payload.get("heartbeat_epoch")
        if type(agent_code) is not int or agent_code < 0 or type(session_epoch) is not int or session_epoch < 0:
            raise ValueError("REGISTER must include compact heartbeat code and epoch")
        binding = self._compact_heartbeat_bindings.get(agent_code)
        if binding is not None and binding[0] != profile.agent_id:
            raise ValueError(f"compact heartbeat code collision for {profile.agent_id}")
        self._compact_heartbeat_bindings[agent_code] = (profile.agent_id, payload["session_id"], session_epoch, -1)
        print(f"[coordinator] registered {profile.agent_id}: {profile.skills}")
        await self._route_ready_tasks()

    async def _handle_state_sync(self, message: dict[str, Any]) -> None:
        """Record a reconnecting agent's running-task view without replaying it."""
        payload = payload_of(message)
        agent_id = payload["agent_id"]
        session_id = payload["session_id"]
        accepted = await self.registry.heartbeat(
            agent_id, session_id, int(payload.get("current_load", 0))
        )
        if not accepted:
            print(f"[coordinator] ignored state sync from unknown/stale {agent_id}")
            return
        active_task_ids = payload.get("active_task_ids", [])
        if not isinstance(active_task_ids, list) or not all(isinstance(item, str) for item in active_task_ids):
            raise ValueError("active_task_ids must be a string list")
        pending_result_task_ids = payload.get("pending_result_task_ids", [])
        if not isinstance(pending_result_task_ids, list) or not all(isinstance(item, str) for item in pending_result_task_ids):
            raise ValueError("pending_result_task_ids must be a string list")
        self._agent_active_tasks[agent_id] = set(active_task_ids)
        known_to_agent = set(active_task_ids) | set(pending_result_task_ids)
        print(
            f"[coordinator] state sync from {agent_id}: {len(active_task_ids)} active, "
            f"{len(pending_result_task_ids)} result pending"
        )
        # An assignment may have been committed locally just as the broker
        # disconnected. If the recovered agent does not know it, redeliver the
        # same task record without creating another routing attempt/reservation.
        for task in self.tasks.assigned_tasks(agent_id):
            if task.spec.task_id not in known_to_agent:
                await self._redeliver_assignment(task, agent_id)

    async def _handle_heartbeat(self, message: dict[str, Any]) -> None:
        payload = payload_of(message)
        accepted = await self.registry.heartbeat(
            payload["agent_id"], payload["session_id"], int(payload.get("current_load", 0))
        )
        if not accepted:
            print(f"[coordinator] ignored stale heartbeat from {payload['agent_id']}")

    async def _handle_compact_heartbeat(self, agent_code: int, session_epoch: int, sequence: int) -> None:
        binding = self._compact_heartbeat_bindings.get(agent_code)
        if binding is None:
            print(f"[coordinator] ignored compact heartbeat from unknown code {agent_code}")
            return
        agent_id, session_id, expected_epoch, last_sequence = binding
        if session_epoch != expected_epoch:
            print(f"[coordinator] ignored compact heartbeat with stale epoch from {agent_id}")
            return
        if sequence <= last_sequence:
            print(f"[coordinator] ignored out-of-order compact heartbeat from {agent_id}")
            return
        accepted = await self.registry.heartbeat(agent_id, session_id)
        if not accepted:
            print(f"[coordinator] ignored compact heartbeat from stale {agent_id}")
            return
        self._compact_heartbeat_bindings[agent_code] = (agent_id, session_id, expected_epoch, sequence)

    async def _handle_new_tasks(self, message: dict[str, Any]) -> None:
        payload = payload_of(message)
        raw_tasks = payload.get("tasks") or [payload["task"]]
        specs = [TaskSpec.from_dict(item) for item in raw_tasks]
        self.tasks.add_many(specs)
        for spec in specs:
            self.task_contexts.initialize_from_spec(spec, replace=True)
        print(f"[coordinator] accepted tasks: {', '.join(task.task_id for task in specs)}")
        await self._route_ready_tasks()

    async def apply_planner_update(
        self,
        specs: list[TaskSpec],
        change_set: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Apply a ToDAG replan without restarting unaffected runtime tasks."""
        async with self._lock:
            sync = self.tasks.apply_planner_update(specs, change_set)
            for agent_id in sync["released_agent_ids"]:
                self.router.release(agent_id)
            for task_id in sync["removed_task_ids"]:
                self.task_contexts.remove(task_id)
                await self.registry.clear_task_edges(task_id)
            reset = set(sync["reset_task_ids"])
            for spec in specs:
                try:
                    self.task_contexts.get(spec.task_id)
                    exists = True
                except KeyError:
                    exists = False
                if spec.task_id in reset or not exists:
                    self.task_contexts.initialize_from_spec(spec, replace=True)
        await self._route_ready_tasks()
        return sync

    async def apply_task_context_message(self, message: TaskMessage) -> TaskContext:
        """Merge one fixed-schema task message into the authoritative cache."""
        async with self._lock:
            return self.task_contexts.apply(message)

    def _require_known_task(self, task_id: str) -> TaskRecord:
        return self.tasks.get(task_id)

    async def _handle_task_context_update(self, message: dict[str, Any]) -> None:
        """Apply one delta, then send receiver-conditioned state differences.

        The Coordinator is authoritative.  It compares the accepted state with
        each receiver's ACK-backed ``KnowledgeDigest`` before deciding what to
        transmit.  Unique low-impact updates are deferred briefly and merged;
        hard constraints/evidence are sent immediately.
        """
        payload = payload_of(message)
        raw_task_message = message if "message_id" in message else payload["task_message"]
        task_message = TaskMessage.from_dict(raw_task_message)
        if "src" in message and message.get("src") != task_message.sender:
            raise ValueError("task context sender does not match envelope source")
        self._require_known_task(task_message.task_id)
        authoritative = self.task_contexts.apply(task_message)

        recipients = {task_message.sender, task_message.receiver} - {"coordinator"}
        for recipient in sorted(recipients):
            candidate = TaskMessage.create(
                message_id=task_message.message_id,
                sender=task_message.sender,
                receiver=recipient,
                task_id=task_message.task_id,
                summary=task_message.summary,
                facts=task_message.facts,
                constraints=task_message.constraints,
                evidence_refs=task_message.evidence_refs,
                priority=task_message.priority,
                ttl=task_message.ttl,
            )
            self.low_entropy_stats.observe_candidate(candidate)
            digest = self.receiver_knowledge.get(recipient, task_message.task_id)
            tailored = self.context_compressor.tailor(
                task_message, authoritative, digest, receiver=recipient
            )
            if tailored is None:
                self.low_entropy_stats.duplicate_drops += 1
                continue
            decision = self.impact_estimator.decide(tailored)
            if decision.action is PolicyAction.SEND:
                await self._publish_context_message(tailored, decision.impact_score)
            elif decision.action is PolicyAction.DEFER:
                merged = self.context_outbox.enqueue(tailored, defer_s=decision.defer_s)
                self.low_entropy_stats.deferred_messages += 1
                if merged:
                    self.low_entropy_stats.merged_messages += 1

    async def _handle_task_context_snapshot_request(self, message: dict[str, Any]) -> None:
        payload = payload_of(message)
        agent_id = payload["agent_id"]
        task_id = payload["task_id"]
        if message.get("src") != agent_id:
            raise ValueError("snapshot requester does not match envelope source")
        if not isinstance(task_id, str):
            raise ValueError("snapshot request requires task_id")
        self._require_known_task(task_id)
        await self._publish_task_context_snapshot(agent_id, task_id)

    async def _publish_task_context_snapshot(
        self,
        receiver: str,
        task_id: str,
    ) -> None:
        """Return a complete replacement context using the fixed schema."""
        snapshot = self.task_contexts.build_snapshot_message(
            sender="coordinator",
            receiver=receiver,
            task_id=task_id,
        )
        # New snapshots share the ordinary context topic.  The ``snapshot:``
        # message-id prefix tells upgraded agents to replace their cache while
        # preserving MQTT ordering with deltas.  Agents still subscribe to the
        # legacy snapshot topic for backward compatibility.
        self.low_entropy_stats.observe_candidate(snapshot)
        await self._publish_context_message(snapshot, 1.0)

    async def _handle_context_ack(self, message: dict[str, Any]) -> None:
        payload = payload_of(message)
        agent_id = payload["agent_id"]
        task_id = payload["task_id"]
        message_id = payload["message_id"]
        if message.get("src") != agent_id:
            raise ValueError("context ACK source mismatch")
        pending = self._context_inflight.pop(message_id, None)
        if pending is None:
            return
        receiver, task_message, fingerprint, _ = pending
        self._context_inflight_hashes.discard((receiver, fingerprint))
        if receiver != agent_id or task_message.task_id != task_id:
            raise ValueError("context ACK does not match the transmitted message")
        self.receiver_knowledge.acknowledge(receiver, task_message)
        self.low_entropy_stats.acked_messages += 1

    async def _handle_context_feedback(self, message: dict[str, Any]) -> None:
        payload = payload_of(message)
        agent_id = payload["agent_id"]
        message_id = payload["message_id"]
        changed_decision = payload["changed_decision"]
        if message.get("src") != agent_id or type(changed_decision) is not bool:
            raise ValueError("invalid context contribution feedback")
        if self.causal_ledger.get(message_id) is None:
            return
        if changed_decision:
            self.low_entropy_stats.feedback_positive += 1
        else:
            self.low_entropy_stats.feedback_negative += 1

    async def _publish_context_message(
        self, message: TaskMessage, predicted_impact: float, *, queue_wait_s: float = 0.0
    ) -> None:
        fingerprint = semantic_fingerprint(message)
        key = (message.receiver, fingerprint)
        if key in self._context_inflight_hashes:
            self.low_entropy_stats.duplicate_drops += 1
            return
        await self.transport.publish(context_inbox(message.receiver), message.to_dict())
        self._context_inflight_hashes.add(key)
        self._context_inflight[message.message_id] = (
            message.receiver, message, fingerprint, time.monotonic()
        )
        self.causal_ledger.record(message, predicted_impact)
        self.low_entropy_stats.observe_sent(message, queue_wait_s)

    async def _low_entropy_flush_loop(self) -> None:
        try:
            while True:
                now = time.monotonic()
                ready, expired = self.context_outbox.pop_ready(now=now)
                self.low_entropy_stats.expired_drops += len(expired)
                for item in ready:
                    decision = self.impact_estimator.decide(item.message)
                    await self._publish_context_message(
                        item.message, decision.impact_score, queue_wait_s=now - item.enqueued_at
                    )
                # Expire semantic in-flight suppression conservatively.  A lost
                # ACK then causes a later resend rather than permanent omission.
                for message_id, (receiver, task_message, fingerprint, sent_at) in list(self._context_inflight.items()):
                    if now - sent_at >= task_message.ttl:
                        self._context_inflight.pop(message_id, None)
                        self._context_inflight_hashes.discard((receiver, fingerprint))
                await asyncio.sleep(0.02)
        except asyncio.CancelledError:
            raise

    def low_entropy_metrics(self) -> dict[str, Any]:
        return self.low_entropy_stats.snapshot()

    async def _handle_result(self, message: dict[str, Any]) -> None:
        if "message_id" in message:
            await self._handle_low_entropy_result(message, TaskMessage.from_dict(message))
            return
        payload = payload_of(message)
        raw_task_message = payload.get("task_message")
        if isinstance(raw_task_message, dict):
            await self._handle_low_entropy_result(message, TaskMessage.from_dict(raw_task_message))
            return
        task_id = payload["task_id"]
        agent_id = payload["agent_id"]
        record = self.tasks.get(task_id)
        if record.status is TaskStatus.COMPLETED:
            if record.assignee != agent_id:
                raise ValueError(f"agent {agent_id} cannot acknowledge completed task {task_id}")
            return
        self.tasks.complete(task_id, agent_id, dict(payload.get("result", {})))
        self.router.release(agent_id)
        print(f"[coordinator] completed {task_id} by {agent_id}")
        await self._route_ready_tasks()

    async def _handle_low_entropy_result(self, envelope_message: dict[str, Any], task_message: TaskMessage) -> None:
        if (
            ("src" in envelope_message and envelope_message.get("src") != task_message.sender)
            or task_message.receiver != "coordinator"
        ):
            raise ValueError("invalid low-entropy result sender or receiver")
        record = self._require_known_task(task_message.task_id)
        if record.status is TaskStatus.COMPLETED:
            if record.assignee != task_message.sender:
                raise ValueError(f"agent {task_message.sender} cannot acknowledge completed {task_message.task_id}")
            return
        context = self.task_contexts.apply(task_message)
        result = {
            "status": "ok",
            "summary": task_message.summary or context.summary,
            "facts": context.to_dict()["facts"],
            "evidence_refs": context.to_dict()["evidence_refs"],
        }
        self.tasks.complete(task_message.task_id, task_message.sender, result)
        self.router.release(task_message.sender)
        print(f"[coordinator] completed {task_message.task_id} by {task_message.sender} (low entropy)")
        await self._route_ready_tasks()

    async def _route_ready_tasks(self) -> None:
        for task in self.tasks.ready_tasks():
            decision, placement = await self._select_route(task)
            if decision is None:
                print(f"[coordinator] no available agent for {task.spec.task_id}; task remains pending")
                continue
            self.tasks.assign(task.spec.task_id, decision.agent_id)
            self.router.reserve(decision.agent_id)
            await self._create_task_edges(task, decision.agent_id)
            snapshot = await self.registry.topology_snapshot()
            assignment = envelope(
                "TASK_ASSIGNMENT",
                "coordinator",
                decision.agent_id,
                task=task.spec.to_dict(),
                topology_version=snapshot.version,
                route_score=decision.score,
                placement=placement.to_dict() if placement is not None else None,
            )
            await self.transport.publish(agent_inbox(decision.agent_id), assignment)
            await self._publish_topology(task.spec.task_id, decision.agent_id, snapshot.version)
            print(f"[coordinator] routed {task.spec.task_id} -> {decision.agent_id} score={decision.score}")

    async def _select_route(self, task: TaskRecord) -> tuple[RouteDecision | None, PlacementDecision | None]:
        """Give an injected scheduler first choice, then retain local fallback."""
        if self.placement_port is None:
            return await self.router.select(task), None
        request = PlacementRequest.from_task(task.spec)
        candidates = await self.router.candidates(task)
        placement = await self.placement_port.select(request, candidates)
        if placement is None:
            return await self.router.select(task), None
        # The response is treated as a proposal until rechecked against current
        # registry state. This protects against a stale external decision.
        decision = await self.router.select(task, allowed_agent_ids={placement.agent_id})
        if decision is None:
            print(
                f"[coordinator] placement target {placement.agent_id} for {task.spec.task_id} "
                "is no longer eligible; using local fallback"
            )
            return await self.router.select(task), None
        return decision, placement

    async def _create_task_edges(self, task: TaskRecord, target_agent: str) -> None:
        sources = [
            self.tasks.get(parent_id).assignee
            for parent_id in task.spec.dependencies
            if self.tasks.get(parent_id).assignee is not None
        ]
        if not sources and task.spec.requested_by != "system":
            sources = [task.spec.requested_by]
        await self.registry.replace_task_edges(
            task.spec.task_id,
            [(source, target_agent) for source in sources],
        )

    async def _publish_topology(self, task_id: str, target_agent: str, version: int) -> None:
        snapshot = await self.registry.topology_snapshot()
        task_edges = [edge for edge in snapshot.task_edges if edge[0] == task_id]
        participants = {target_agent} | {src for _, src, _ in task_edges}
        message = envelope("TOPOLOGY_UPDATE", "coordinator", topology_version=version, task_id=task_id, edges=task_edges)
        for agent_id in participants:
            await self.transport.publish(topology_topic(agent_id), message)

    async def _redeliver_assignment(self, task: TaskRecord, agent_id: str) -> None:
        snapshot = await self.registry.topology_snapshot()
        assignment = envelope(
            "TASK_ASSIGNMENT",
            "coordinator",
            agent_id,
            task=task.spec.to_dict(),
            topology_version=snapshot.version,
            route_score=None,
            redelivery=True,
        )
        await self.transport.publish(agent_inbox(agent_id), assignment)
        await self._publish_topology(task.spec.task_id, agent_id, snapshot.version)
        print(f"[coordinator] redelivered {task.spec.task_id} -> {agent_id} after state sync")

    async def _watchdog_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.watchdog_interval)
                async with self._lock:
                    # Losing the broker is not evidence that each agent failed.
                    # Hold liveness decisions until reconnected agents have had a
                    # bounded window to re-register and report their state.
                    if not self._broker_connected or time.monotonic() < self._suspend_expiry_until:
                        continue
                    offline = await self.registry.sweep_expired()
                    for agent_id in offline:
                        released = self.tasks.release_agent_tasks(agent_id)
                        for task in released:
                            self.router.release(agent_id)
                            await self.registry.clear_task_edges(task.spec.task_id)
                            print(f"[coordinator] released {task.spec.task_id}; {agent_id} is offline")
                    if offline:
                        await self._route_ready_tasks()
        except asyncio.CancelledError:
            raise

    async def _on_transport_connection(self, connected: bool, reconnected: bool) -> None:
        self._broker_connected = connected
        if not connected:
            self._suspend_expiry_until = float("inf")
            print("[coordinator] broker unavailable; liveness expiry is paused")
            return
        # The startup registration path does not need a recovery window. The
        # grace period only protects existing leases after an actual reconnect.
        self._suspend_expiry_until = time.monotonic() + self.resync_grace_s if reconnected else 0.0
        if reconnected:
            print("[coordinator] broker recovered; requesting agent state synchronization")
            try:
                await self.transport.publish(
                    STATE_SYNC_REQUEST_TOPIC,
                    envelope("STATE_SYNC_REQUEST", "coordinator", requested_at=time.time()),
                )
            except RuntimeError as exc:
                print(f"[coordinator] unable to request state synchronization: {exc}")

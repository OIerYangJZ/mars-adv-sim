"""Independent Agent process runtime for local-bus or MQTT execution."""

from __future__ import annotations

import asyncio
import contextlib
import secrets
import uuid
from typing import Any

from .coordinator import (
    HEARTBEAT_TOPIC,
    REGISTER_TOPIC,
    RESULT_TOPIC,
    STATE_SYNC_REQUEST_TOPIC,
    STATE_SYNC_TOPIC,
    TASK_CONTEXT_SNAPSHOT_REQUEST_TOPIC,
    TASK_CONTEXT_UPDATE_TOPIC,
    CONTEXT_ACK_TOPIC,
    CONTEXT_FEEDBACK_TOPIC,
    agent_inbox,
    context_inbox,
    context_snapshot_inbox,
    topology_topic,
)
from .dedup import MessageDeduplicator
from .models import AgentProfile
from .protocol import compact_agent_code, compact_heartbeat, envelope, payload_of
from .task_context import TaskContextStore
from .task_messages import TaskMessage
from .transport import MessageTransport


class AgentRuntime:
    def __init__(self, profile: AgentProfile, transport: MessageTransport, heartbeat_interval: float = 3.0) -> None:
        self.profile = profile
        self.transport = transport
        self.heartbeat_interval = heartbeat_interval
        self.session_id = uuid.uuid4().hex
        self.heartbeat_agent_code = compact_agent_code(profile.agent_id)
        self.heartbeat_epoch = secrets.randbits(32)
        self._heartbeat_sequence = 0
        self.current_load = 0
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._execution_tasks: set[asyncio.Task[None]] = set()
        self._active_task_ids: set[str] = set()
        self._started_task_ids: set[str] = set()
        self._pending_results: dict[str, dict[str, Any]] = {}
        self.task_contexts = TaskContextStore()
        self._deduplicator = MessageDeduplicator()
        self._presence_lock = asyncio.Lock()
        self._context_messages_by_task: dict[str, list[str]] = {}

    @staticmethod
    def subscriptions(agent_id: str) -> tuple[str, ...]:
        return (
            agent_inbox(agent_id),
            context_inbox(agent_id),
            context_snapshot_inbox(agent_id),
            topology_topic(agent_id),
            STATE_SYNC_REQUEST_TOPIC,
        )

    async def start(self) -> None:
        self.transport.add_connection_listener(self._on_transport_connection)
        await self.transport.start(self.on_message)
        await self._announce_presence("startup")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name=f"heartbeat:{self.profile.agent_id}")
        print(f"[agent:{self.profile.agent_id}] started")

    def _profile_payload(self) -> dict[str, Any]:
        return self.profile.to_dict()

    async def _announce_presence(self, reason: str) -> None:
        """Restore coordinator-visible state after initial connect or recovery."""
        async with self._presence_lock:
            await self.transport.wait_until_connected()
            await self.transport.publish(
                REGISTER_TOPIC,
                envelope(
                    "REGISTER",
                    self.profile.agent_id,
                    profile=self._profile_payload(),
                    session_id=self.session_id,
                    heartbeat_agent_code=self.heartbeat_agent_code,
                    heartbeat_epoch=self.heartbeat_epoch,
                ),
            )
            await self.transport.publish(
                STATE_SYNC_TOPIC,
                envelope(
                    "STATE_SYNC",
                    self.profile.agent_id,
                    "coordinator",
                    agent_id=self.profile.agent_id,
                    session_id=self.session_id,
                    current_load=self.current_load,
                    active_task_ids=sorted(self._active_task_ids),
                    pending_result_task_ids=sorted(self._pending_results),
                    reason=reason,
                ),
            )
            await self._publish_heartbeat()
            await self._flush_pending_results()

    async def _publish_heartbeat(self) -> None:
        self._heartbeat_sequence += 1
        await self.transport.publish(
            HEARTBEAT_TOPIC,
            compact_heartbeat(
                self.heartbeat_agent_code,
                self.heartbeat_epoch,
                self._heartbeat_sequence,
            ),
        )

    async def _flush_pending_results(self) -> None:
        """Publish pending QoS-1 results; failed publishes remain for recovery."""
        for task_id, message in list(self._pending_results.items()):
            try:
                await self.transport.publish(RESULT_TOPIC, message)
            except RuntimeError as exc:
                print(f"[agent:{self.profile.agent_id}] result {task_id} remains pending: {exc}")
                return
            if self._pending_results.get(task_id) is message:
                self._pending_results.pop(task_id, None)
                self._started_task_ids.discard(task_id)

    async def stop(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
        for task in list(self._execution_tasks):
            task.cancel()
        await self.transport.stop()

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                if self.transport.is_connected:
                    try:
                        await self._publish_heartbeat()
                    except RuntimeError as exc:
                        print(f"[agent:{self.profile.agent_id}] heartbeat deferred: {exc}")
                await asyncio.sleep(self.heartbeat_interval)
        except asyncio.CancelledError:
            raise

    async def on_message(self, topic: str, message: dict[str, Any]) -> None:
        logical_message_id = message.get("id", message.get("message_id"))
        if self._deduplicator.is_duplicate(logical_message_id):
            print(f"[agent:{self.profile.agent_id}] ignored duplicate {message.get('type', 'message')}")
            return
        if topic == context_inbox(self.profile.agent_id):
            await self._accept_task_context(message)
            return
        if topic == context_snapshot_inbox(self.profile.agent_id):
            await self._accept_task_context(message, replace=True)
            return
        if topic == STATE_SYNC_REQUEST_TOPIC:
            await self._announce_presence("coordinator_request")
            return
        if topic == topology_topic(self.profile.agent_id):
            payload = payload_of(message)
            print(f"[agent:{self.profile.agent_id}] topology v{payload['topology_version']} for {payload['task_id']}: {payload['edges']}")
            return
        if topic != agent_inbox(self.profile.agent_id):
            return
        payload = payload_of(message)
        message_type = message.get("type")
        if message_type != "TASK_ASSIGNMENT":
            return
        task = payload["task"]
        self._start_execution(task)

    def _start_execution(self, task: dict[str, Any]) -> None:
        task_id = task["task_id"]
        if task_id in self._started_task_ids:
            return
        self._started_task_ids.add(task_id)
        execution = asyncio.create_task(self._execute(task), name=f"task:{task_id}")
        self._execution_tasks.add(execution)
        execution.add_done_callback(self._execution_tasks.discard)

    async def _accept_task_context(self, raw_message: dict[str, Any], *, replace: bool = False) -> None:
        """Apply a coordinator-approved delta and ACK the resulting knowledge.

        New snapshots use the same context topic and a ``snapshot:`` message-id
        prefix, which preserves ordering with deltas without enlarging the fixed
        ten-field TaskMessage body.  The legacy snapshot topic remains accepted.
        """
        task_message = TaskMessage.from_dict(raw_message)
        if task_message.receiver != self.profile.agent_id:
            raise ValueError("task context receiver does not match this agent")
        replace = replace or task_message.message_id.startswith("snapshot:")
        self.task_contexts.apply(task_message, replace=replace)
        recent = self._context_messages_by_task.setdefault(task_message.task_id, [])
        recent.append(task_message.message_id)
        del recent[:-64]
        try:
            await self.transport.publish(
                CONTEXT_ACK_TOPIC,
                envelope(
                    "TASK_CONTEXT_ACK",
                    self.profile.agent_id,
                    "coordinator",
                    agent_id=self.profile.agent_id,
                    task_id=task_message.task_id,
                    message_id=task_message.message_id,
                ),
            )
        except RuntimeError as exc:
            # Missing ACK only makes the coordinator conservative and may cause
            # a harmless resend; it must not roll back an already-applied delta.
            print(f"[agent:{self.profile.agent_id}] context ACK deferred/lost: {exc}")

    async def report_context_contribution(
        self, task_id: str, message_id: str, *, changed_decision: bool
    ) -> None:
        """Report whether an accepted context message changed a later decision.

        This is an explicit learning/telemetry hook rather than an automatic
        claim: callers should invoke it only when the downstream decision can be
        compared or attributed.
        """
        if message_id not in self._context_messages_by_task.get(task_id, []):
            raise ValueError("message_id is not an accepted context message for this task")
        await self.transport.publish(
            CONTEXT_FEEDBACK_TOPIC,
            envelope(
                "TASK_CONTEXT_FEEDBACK",
                self.profile.agent_id,
                "coordinator",
                agent_id=self.profile.agent_id,
                task_id=task_id,
                message_id=message_id,
                changed_decision=changed_decision,
            ),
        )

    async def request_task_context_snapshot(self, task_id: str) -> None:
        """Request a full replacement context after restart or local cache loss."""
        await self.transport.publish(
            TASK_CONTEXT_SNAPSHOT_REQUEST_TOPIC,
            envelope(
                "TASK_CONTEXT_SNAPSHOT_REQUEST",
                self.profile.agent_id,
                "coordinator",
                agent_id=self.profile.agent_id,
                task_id=task_id,
            ),
        )

    async def publish_task_context_delta(self, task_message: TaskMessage) -> None:
        """Submit one low-entropy task delta through the Coordinator gateway."""
        if task_message.sender != self.profile.agent_id:
            raise ValueError("task context sender does not match this agent")
        await self.transport.publish(TASK_CONTEXT_UPDATE_TOPIC, task_message.to_dict())

    async def _execute(self, task: dict[str, Any]) -> None:
        task_id = task["task_id"]
        required = set(task.get("required_skills", []))
        if not required.issubset(set(self.profile.skills)):
            return
        self._active_task_ids.add(task_id)
        self.current_load += 1
        print(f"[agent:{self.profile.agent_id}] executing {task_id}: {task['title']}")
        try:
            await asyncio.sleep(float(task.get("simulated_duration_s", 1.0)))
            result = {
                # task_id already resolves to the title in the coordinator's
                # task table, so it is not repeated in this low-entropy result.
                "summary": "执行完成",
            }
            self.task_contexts.ensure(task_id)
            result_message = TaskMessage.create(
                message_id=uuid.uuid4().hex,
                sender=self.profile.agent_id,
                receiver="coordinator",
                task_id=task_id,
                summary=result["summary"],
                facts=(),
                priority=int(task.get("priority", 5)),
            )
            self._pending_results[task_id] = result_message.to_dict()
            await self._flush_pending_results()
        finally:
            self._active_task_ids.discard(task_id)
            self.current_load -= 1

    async def _on_transport_connection(self, connected: bool, reconnected: bool) -> None:
        if not connected:
            print(f"[agent:{self.profile.agent_id}] broker unavailable; preserving local execution state")
            return
        if reconnected:
            print(f"[agent:{self.profile.agent_id}] broker recovered; restoring registration and state")
            try:
                await self._announce_presence("mqtt_reconnect")
            except RuntimeError as exc:
                print(f"[agent:{self.profile.agent_id}] state restoration deferred: {exc}")

"""Transport adapters: local TCP pub/sub for development and MQTT for deployment."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from .observability import JsonlMessageObserver, MessageEvent, MessageObserver, NullMessageObserver
from .settings import TransportSettings

TopicHandler = Callable[[str, dict[str, Any]], Awaitable[None]]
ConnectionListener = Callable[[bool, bool], Awaitable[None]]


class MessageTransport(Protocol):
    """Transport contract shared by process roles.

    A connection listener receives ``(connected, reconnected)``. ``connected``
    is false on loss; ``reconnected`` is true only for successful connections
    after the first one in the current process lifetime.
    """

    @property
    def is_connected(self) -> bool: ...
    def add_connection_listener(self, listener: ConnectionListener) -> None: ...
    async def wait_until_connected(self) -> None: ...
    async def start(self, handler: TopicHandler) -> None: ...
    async def publish(self, topic: str, message: dict[str, Any]) -> None: ...
    async def stop(self) -> None: ...


class LocalBusTransport:
    """JSON-lines pub/sub client used to test multiple local processes."""

    def __init__(
        self,
        client_id: str,
        subscriptions: tuple[str, ...],
        host: str,
        port: int,
        observer: MessageObserver | None = None,
    ) -> None:
        self.client_id = client_id
        self.subscriptions = subscriptions
        self.host = host
        self.port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._handler: TopicHandler | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._connected = asyncio.Event()
        self._connection_listeners: list[ConnectionListener] = []
        self._observer = observer or NullMessageObserver()

    def _observe(self, direction: str, topic: str, message: dict[str, Any], status: str, duplicate: bool = False) -> None:
        self._observer.record(
            MessageEvent.from_message(
                direction=direction,  # type: ignore[arg-type]
                transport="local",
                client_id=self.client_id,
                topic=topic,
                message=message,
                qos=None,
                delivery_status=status,
                duplicate_flag=duplicate,
            )
        )

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    def add_connection_listener(self, listener: ConnectionListener) -> None:
        self._connection_listeners.append(listener)

    async def wait_until_connected(self) -> None:
        await self._connected.wait()

    async def _notify_connection(self, connected: bool, reconnected: bool = False) -> None:
        for listener in tuple(self._connection_listeners):
            await listener(connected, reconnected)

    async def start(self, handler: TopicHandler) -> None:
        self._handler = handler
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        self._connected.set()
        try:
            await self._send({"op": "hello", "client_id": self.client_id})
            for topic in self.subscriptions:
                await self._send({"op": "subscribe", "topic": topic})
        except Exception:
            self._connected.clear()
            raise
        await self._notify_connection(True)
        self._reader_task = asyncio.create_task(self._read_loop(), name=f"local-bus:{self.client_id}")

    async def _send(self, frame: dict[str, Any]) -> None:
        if self._writer is None or not self.is_connected:
            raise RuntimeError("transport is not connected")
        self._writer.write((json.dumps(frame, ensure_ascii=False) + "\n").encode("utf-8"))
        await self._writer.drain()

    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
            while line := await self._reader.readline():
                frame = json.loads(line.decode("utf-8"))
                if frame.get("op") == "message" and self._handler is not None:
                    self._observe("inbound", frame["topic"], frame["payload"], "received")
                    await self._handler(frame["topic"], frame["payload"])
        except asyncio.CancelledError:
            raise
        except (ConnectionError, json.JSONDecodeError) as exc:
            print(f"[transport:{self.client_id}] local bus disconnected: {exc}")
        finally:
            if self._connected.is_set():
                self._connected.clear()
                await self._notify_connection(False)

    async def publish(self, topic: str, message: dict[str, Any]) -> None:
        try:
            await self._send({"op": "publish", "topic": topic, "payload": message})
        except Exception:
            self._observe("outbound", topic, message, "publish_failed")
            raise
        self._observe("outbound", topic, message, "published")

    async def stop(self) -> None:
        self._connected.clear()
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None
        if self._writer is not None:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None


class MqttTransport:
    """MQTT transport with reconnect, re-subscribe, and connection events."""

    def __init__(
        self,
        client_id: str,
        subscriptions: tuple[str, ...],
        host: str,
        port: int,
        protocol: str,
        reconnect_min_delay: int = 1,
        reconnect_max_delay: int = 30,
        observer: MessageObserver | None = None,
    ) -> None:
        if not host:
            raise ValueError("MQTT_HOST is empty. Fill the VM Broker IP before using --transport mqtt.")
        if reconnect_min_delay < 1 or reconnect_max_delay < reconnect_min_delay:
            raise ValueError("invalid MQTT reconnect delay range")
        self.client_id = client_id
        self.subscriptions = subscriptions
        self.host = host
        self.port = port
        self.protocol = protocol
        self.reconnect_min_delay = reconnect_min_delay
        self.reconnect_max_delay = reconnect_max_delay
        self._client: Any = None
        self._handler: TopicHandler | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._connected = asyncio.Event()
        self._connection_listeners: list[ConnectionListener] = []
        self._has_connected = False
        self._stopping = False
        self._observer = observer or NullMessageObserver()

    def _observe(
        self,
        direction: str,
        topic: str,
        message: dict[str, Any],
        status: str,
        qos: int = 1,
        duplicate: bool = False,
    ) -> None:
        self._observer.record(
            MessageEvent.from_message(
                direction=direction,  # type: ignore[arg-type]
                transport="mqtt",
                client_id=self.client_id,
                topic=topic,
                message=message,
                qos=qos,
                delivery_status=status,
                duplicate_flag=duplicate,
            )
        )

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    def add_connection_listener(self, listener: ConnectionListener) -> None:
        self._connection_listeners.append(listener)

    async def wait_until_connected(self) -> None:
        await self._connected.wait()

    def _schedule_connection_event(self, connected: bool, reconnected: bool = False) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        asyncio.run_coroutine_threadsafe(self._handle_connection_event(connected, reconnected), self._loop)

    async def _handle_connection_event(self, connected: bool, reconnected: bool) -> None:
        if connected:
            self._connected.set()
        else:
            self._connected.clear()
        for listener in tuple(self._connection_listeners):
            try:
                await listener(connected, reconnected)
            except Exception as exc:  # A listener cannot disable transport recovery.
                print(f"[transport:{self.client_id}] connection listener failed: {exc}")

    @staticmethod
    def _reason_code_ok(reason_code: Any) -> bool:
        try:
            return int(reason_code) == 0
        except (TypeError, ValueError):
            return getattr(reason_code, "value", None) == 0

    async def start(self, handler: TopicHandler) -> None:
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise RuntimeError("MQTT support requires: pip install -r requirements.txt") from exc

        self._handler = handler
        self._loop = asyncio.get_running_loop()
        self._stopping = False
        # A stable client ID plus persistent session lets the broker preserve
        # QoS-1 in-flight state. Application-level ID de-duplication remains
        # necessary because MQTT QoS 1 is intentionally at-least-once.
        self._client = mqtt.Client(client_id=self.client_id, clean_session=False, transport=self.protocol)
        self._client.reconnect_delay_set(self.reconnect_min_delay, self.reconnect_max_delay)

        def on_connect(client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any = None) -> None:
            if not self._reason_code_ok(reason_code):
                print(f"[transport:{self.client_id}] MQTT connect rejected: {reason_code}")
                self._schedule_connection_event(False)
                return
            # MQTT subscriptions are connection state; repeat them after every
            # reconnect even when the broker restores a persistent session.
            for topic in self.subscriptions:
                result, _ = client.subscribe(topic, qos=1)
                if result != mqtt.MQTT_ERR_SUCCESS:
                    print(f"[transport:{self.client_id}] MQTT subscribe failed for {topic}: {result}")
            reconnected = self._has_connected
            self._has_connected = True
            self._schedule_connection_event(True, reconnected)

        def on_disconnect(client: Any, userdata: Any, *args: Any) -> None:
            if not self._stopping:
                print(f"[transport:{self.client_id}] MQTT disconnected; reconnecting")
                self._schedule_connection_event(False)

        def on_connect_fail(client: Any, userdata: Any, *args: Any) -> None:
            if not self._stopping:
                print(f"[transport:{self.client_id}] MQTT connection failed; retrying")
                self._schedule_connection_event(False)

        def on_message(client: Any, userdata: Any, message: Any) -> None:
            try:
                payload = json.loads(message.payload.decode("utf-8"))
                self._observe(
                    "inbound",
                    message.topic,
                    payload,
                    "received",
                    qos=int(getattr(message, "qos", 1)),
                    duplicate=bool(getattr(message, "dup", False)),
                )
                if self._handler is not None and self._loop is not None:
                    asyncio.run_coroutine_threadsafe(self._handler(message.topic, payload), self._loop)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                print(f"[transport:{self.client_id}] invalid MQTT payload: {exc}")

        self._client.on_connect = on_connect
        self._client.on_disconnect = on_disconnect
        self._client.on_connect_fail = on_connect_fail
        self._client.on_message = on_message
        self._client.connect_async(self.host, self.port, keepalive=30)
        self._client.loop_start()
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=10)
        except TimeoutError as exc:
            # The paho network thread continues retrying after startup. Callers
            # fail fast so a service supervisor can report an unavailable broker.
            raise RuntimeError(f"MQTT initial connection timed out: {self.host}:{self.port}") from exc

    async def publish(self, topic: str, message: dict[str, Any]) -> None:
        if self._client is None:
            self._observe("outbound", topic, message, "transport_not_started")
            raise RuntimeError("transport is not started")
        if not self.is_connected:
            self._observe("outbound", topic, message, "transport_disconnected")
            raise RuntimeError("MQTT transport is disconnected")
        result = self._client.publish(topic, json.dumps(message, ensure_ascii=False), qos=1)
        if result.rc != 0:
            self._observe("outbound", topic, message, "publish_failed")
            raise RuntimeError(f"MQTT publish failed with code {result.rc}")
        self._observe("outbound", topic, message, "published")

    async def stop(self) -> None:
        self._stopping = True
        self._connected.clear()
        if self._client is not None:
            self._client.disconnect()
            self._client.loop_stop()
            self._client = None


def create_transport(settings: TransportSettings, client_id: str, subscriptions: tuple[str, ...]) -> MessageTransport:
    observer: MessageObserver
    if settings.observe_messages:
        observer = JsonlMessageObserver(settings.observation_dir, client_id)
    else:
        observer = NullMessageObserver()
    if settings.mode == "local":
        return LocalBusTransport(client_id, subscriptions, settings.local_host, settings.local_port, observer)
    if settings.mode == "mqtt":
        return MqttTransport(
            client_id,
            subscriptions,
            settings.mqtt_host,
            settings.mqtt_port,
            settings.mqtt_protocol,
            settings.mqtt_reconnect_min_delay,
            settings.mqtt_reconnect_max_delay,
            observer,
        )
    raise ValueError(f"unknown transport mode: {settings.mode}")

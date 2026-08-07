"""Shared transport configuration for coordinator, agents, and submitters."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass


# 当前虚拟机 MQTT Broker。环境变量或命令行参数可覆盖这些默认值。
DEFAULT_MQTT_HOST = "192.168.175.129"
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTT_PROTOCOL = "tcp"
DEFAULT_MQTT_RECONNECT_MIN_DELAY = 1
DEFAULT_MQTT_RECONNECT_MAX_DELAY = 30


@dataclass(frozen=True)
class TransportSettings:
    mode: str = "mqtt"
    mqtt_host: str = DEFAULT_MQTT_HOST
    mqtt_port: int = DEFAULT_MQTT_PORT
    mqtt_protocol: str = DEFAULT_MQTT_PROTOCOL
    mqtt_reconnect_min_delay: int = DEFAULT_MQTT_RECONNECT_MIN_DELAY
    mqtt_reconnect_max_delay: int = DEFAULT_MQTT_RECONNECT_MAX_DELAY
    observe_messages: bool = False
    observation_dir: str = "telemetry"
    local_host: str = "127.0.0.1"
    local_port: int = 8765


def add_transport_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--transport", choices=("local", "mqtt"), default=os.getenv("TRANSPORT", "mqtt"))
    parser.add_argument("--mqtt-host", default=os.getenv("MQTT_HOST", DEFAULT_MQTT_HOST), help="MQTT Broker IP")
    parser.add_argument("--mqtt-port", type=int, default=int(os.getenv("MQTT_PORT", str(DEFAULT_MQTT_PORT))))
    parser.add_argument(
        "--mqtt-protocol",
        choices=("tcp", "websockets"),
        default=os.getenv("MQTT_PROTOCOL", DEFAULT_MQTT_PROTOCOL),
        help="普通 MQTT 选 tcp；Broker 的 WebSocket 监听端口选 websockets",
    )
    parser.add_argument(
        "--mqtt-reconnect-min-delay",
        type=int,
        default=int(os.getenv("MQTT_RECONNECT_MIN_DELAY", str(DEFAULT_MQTT_RECONNECT_MIN_DELAY))),
        help="MQTT reconnect initial backoff in seconds",
    )
    parser.add_argument(
        "--mqtt-reconnect-max-delay",
        type=int,
        default=int(os.getenv("MQTT_RECONNECT_MAX_DELAY", str(DEFAULT_MQTT_RECONNECT_MAX_DELAY))),
        help="MQTT reconnect maximum backoff in seconds",
    )
    parser.add_argument("--local-host", default=os.getenv("LOCAL_BUS_HOST", "127.0.0.1"))
    parser.add_argument("--local-port", type=int, default=int(os.getenv("LOCAL_BUS_PORT", "8765")))
    parser.add_argument(
        "--observe-messages",
        action="store_true",
        default=os.getenv("OBSERVE_MESSAGES", "false").strip().lower() in {"1", "true", "yes", "on"},
        help="write raw communication events to JSONL files; does not compute statistics",
    )
    parser.add_argument(
        "--observation-dir",
        default=os.getenv("OBSERVATION_DIR", "telemetry"),
        help="directory for per-process JSONL communication event files",
    )


def transport_settings_from_args(args: argparse.Namespace) -> TransportSettings:
    return TransportSettings(
        mode=args.transport,
        mqtt_host=args.mqtt_host.strip(),
        mqtt_port=args.mqtt_port,
        mqtt_protocol=args.mqtt_protocol,
        mqtt_reconnect_min_delay=args.mqtt_reconnect_min_delay,
        mqtt_reconnect_max_delay=args.mqtt_reconnect_max_delay,
        observe_messages=args.observe_messages,
        observation_dir=args.observation_dir,
        local_host=args.local_host,
        local_port=args.local_port,
    )

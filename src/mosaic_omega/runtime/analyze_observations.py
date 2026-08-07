"""Offline summaries for raw communication-event JSONL observation files."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


def percentile(values: Iterable[int], fraction: float) -> int:
    ordered = sorted(values)
    if not ordered:
        return 0
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return ordered[index]


def load_events(directory: Path) -> tuple[list[dict[str, Any]], list[str]]:
    events: list[dict[str, Any]] = []
    errors: list[str] = []
    for path in sorted(directory.glob("communication-*.jsonl")):
        with path.open("r", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, start=1):
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    errors.append(f"{path.name}:{line_number}: {exc.msg}")
                    continue
                if not isinstance(event, dict):
                    errors.append(f"{path.name}:{line_number}: event is not an object")
                    continue
                events.append(event)
    return events, errors


def event_type(event: dict[str, Any]) -> str:
    value = event.get("message_type")
    return value if isinstance(value, str) and value else "<unknown>"


def event_bytes(event: dict[str, Any], key: str) -> int:
    value = event.get(key, 0)
    return value if isinstance(value, int) and value >= 0 else 0


def summarize(events: list[dict[str, Any]], source_files: int, parse_errors: list[str]) -> dict[str, Any]:
    outbound = [event for event in events if event.get("direction") == "outbound"]
    inbound = [event for event in events if event.get("direction") == "inbound"]
    successful_outbound = [event for event in outbound if event.get("delivery_status") == "published"]
    published_ids = {event.get("message_id") for event in successful_outbound if isinstance(event.get("message_id"), str)}
    received_ids = {event.get("message_id") for event in inbound if isinstance(event.get("message_id"), str)}
    published_delivery_keys = {
        (event.get("message_id"), event.get("topic"))
        for event in successful_outbound
        if isinstance(event.get("message_id"), str) and isinstance(event.get("topic"), str)
    }

    by_type: dict[str, dict[str, Any]] = {}
    all_types = sorted({event_type(event) for event in events})
    for kind in all_types:
        kind_events = [event for event in events if event_type(event) == kind]
        sent = [event for event in kind_events if event.get("direction") == "outbound" and event.get("delivery_status") == "published"]
        received = [event for event in kind_events if event.get("direction") == "inbound"]
        sent_ids = [event.get("message_id") for event in sent if isinstance(event.get("message_id"), str)]
        fields = Counter(
            path
            for event in kind_events
            for path in event.get("field_paths", [])
            if isinstance(path, str)
        )
        by_type[kind] = {
            "outbound_published_events": len(sent),
            "outbound_unique_message_ids": len(set(sent_ids)),
            "outbound_payload_bytes": sum(event_bytes(event, "payload_bytes") for event in sent),
            "outbound_message_bytes": sum(event_bytes(event, "message_bytes") for event in sent),
            "outbound_avg_message_bytes": round(
                sum(event_bytes(event, "message_bytes") for event in sent) / len(sent), 2
            ) if sent else 0,
            "outbound_p95_message_bytes": percentile((event_bytes(event, "message_bytes") for event in sent), 0.95),
            "inbound_received_events": len(received),
            "mqtt_duplicate_flag_events": sum(bool(event.get("duplicate_flag")) for event in kind_events),
            "top_field_paths": [
                {"path": path, "event_count": count, "occurrence_rate": round(count / len(kind_events), 4)}
                for path, count in fields.most_common(12)
            ],
        }

    by_topic: dict[str, dict[str, Any]] = {}
    for topic in sorted({str(event.get("topic", "<unknown>")) for event in successful_outbound}):
        topic_events = [event for event in successful_outbound if str(event.get("topic", "<unknown>")) == topic]
        by_topic[topic] = {
            "published_events": len(topic_events),
            "message_bytes": sum(event_bytes(event, "message_bytes") for event in topic_events),
            "payload_bytes": sum(event_bytes(event, "payload_bytes") for event in topic_events),
        }

    status_counts = Counter(str(event.get("delivery_status", "<unknown>")) for event in events)
    timestamps = sorted(
        float(event["observed_at"])
        for event in events
        if isinstance(event.get("observed_at"), (int, float))
    )
    return {
        "source_files": source_files,
        "parse_error_count": len(parse_errors),
        "parse_errors": parse_errors[:20],
        "raw_event_count": len(events),
        "observation_started_at": timestamps[0] if timestamps else None,
        "observation_ended_at": timestamps[-1] if timestamps else None,
        "observation_duration_s": round(timestamps[-1] - timestamps[0], 3) if timestamps else 0,
        "outbound_event_count": len(outbound),
        "inbound_event_count": len(inbound),
        "published_event_count": len(successful_outbound),
        "published_unique_message_count": len(published_ids),
        "published_message_bytes": sum(event_bytes(event, "message_bytes") for event in successful_outbound),
        "published_payload_bytes": sum(event_bytes(event, "payload_bytes") for event in successful_outbound),
        "published_to_received_id_match_count": len(published_ids & received_ids),
        "published_to_received_id_match_rate": round(len(published_ids & received_ids) / len(published_ids), 4) if published_ids else 0,
        # One logical topology update can be intentionally fanned out to more
        # than one topic using the same message ID. A retry is only a repeated
        # publication to the same topic with the same message ID.
        "same_topic_repeated_publication_count": len(successful_outbound) - len(published_delivery_keys),
        "multi_topic_fanout_additional_publication_count": len(published_delivery_keys) - len(published_ids),
        "mqtt_duplicate_flag_event_count": sum(bool(event.get("duplicate_flag")) for event in events),
        "delivery_status_counts": dict(sorted(status_counts.items())),
        "by_message_type": by_type,
        "by_outbound_topic": by_topic,
        "measurement_note": (
            "message_bytes and payload_bytes are UTF-8 JSON application-message sizes. "
            "They exclude MQTT/TCP/TLS framing. Field values are intentionally not recorded, "
            "so this report measures field structure occurrence but cannot yet calculate value-level "
            "repetition or state-change ratios."
        ),
    }


def markdown_report(summary: dict[str, Any], source: Path) -> str:
    lines = [
        "# 通信观测基线汇总",
        "",
        f"数据目录：`{source}`",
        "",
        "## 总体",
        "",
        f"- 原始事件数：{summary['raw_event_count']}",
        f"- 观测持续时间：{summary['observation_duration_s']} 秒",
        f"- 成功发布事件数：{summary['published_event_count']}",
        f"- 去重后的发布消息数：{summary['published_unique_message_count']}",
        f"- 成功发布的应用消息字节数：{summary['published_message_bytes']}",
        f"- 发布消息与接收记录的 ID 匹配率：{summary['published_to_received_id_match_rate']:.2%}",
        f"- MQTT `dup` 标记事件数：{summary['mqtt_duplicate_flag_event_count']}",
        f"- 同 ID 跨 Topic 扇出附加发布数：{summary['multi_topic_fanout_additional_publication_count']}",
        f"- 同 ID、同 Topic 的重复发布数：{summary['same_topic_repeated_publication_count']}",
        "",
        "## 按消息类型（以发送侧 published 为主）",
        "",
        "| 类型 | 发布事件 | 唯一消息 | 发布字节 | 平均字节 | P95 字节 | 接收事件 | MQTT dup |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for kind, data in summary["by_message_type"].items():
        lines.append(
            f"| `{kind}` | {data['outbound_published_events']} | {data['outbound_unique_message_ids']} | "
            f"{data['outbound_message_bytes']} | {data['outbound_avg_message_bytes']} | "
            f"{data['outbound_p95_message_bytes']} | {data['inbound_received_events']} | "
            f"{data['mqtt_duplicate_flag_events']} |"
        )
    lines.extend([
        "",
        "## 说明与边界",
        "",
        "- 同一条 MQTT 业务消息通常会在发送端和接收端各记录一次；发布流量以 `outbound + published` 为准。",
        "- 字节数为当前 JSON 应用消息 UTF-8 长度，不含 MQTT、TCP 或 TLS 协议头。",
        "- 当前观测层不保存字段值，因此本报告只能显示字段结构的出现率；值级重复率和状态变化比例需在下一阶段增加脱敏状态比较器。",
        "",
        "## 高频字段结构（每类消息前 12 项）",
        "",
    ])
    for kind, data in summary["by_message_type"].items():
        lines.extend([f"### `{kind}`", ""])
        for field in data["top_field_paths"]:
            lines.append(f"- `{field['path']}`：{field['event_count']} 次，出现率 {field['occurrence_rate']:.2%}")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize raw communication observation JSONL files")
    parser.add_argument("observation_dir", type=Path)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()
    source = args.observation_dir.resolve()
    if not source.is_dir():
        raise SystemExit(f"observation directory does not exist: {source}")
    events, parse_errors = load_events(source)
    summary = summarize(events, len(list(source.glob("communication-*.jsonl"))), parse_errors)
    output = (args.output_dir or source).resolve()
    output.mkdir(parents=True, exist_ok=True)
    (output / "communication_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output / "通信观测基线汇总.md").write_text(markdown_report(summary, source), encoding="utf-8")
    print(json.dumps({
        "raw_event_count": summary["raw_event_count"],
        "published_unique_message_count": summary["published_unique_message_count"],
        "published_message_bytes": summary["published_message_bytes"],
        "report": str(output / "通信观测基线汇总.md"),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()

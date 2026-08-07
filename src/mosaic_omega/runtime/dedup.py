"""Bounded, time-based message de-duplication for MQTT QoS 1 delivery."""

from __future__ import annotations

import time
from collections import OrderedDict


class MessageDeduplicator:
    """Remembers recent envelope IDs without retaining an unbounded history."""

    def __init__(self, ttl_s: float = 600.0, max_entries: int = 4096) -> None:
        self.ttl_s = ttl_s
        self.max_entries = max_entries
        self._seen: OrderedDict[str, float] = OrderedDict()

    def is_duplicate(self, message_id: object) -> bool:
        """Return whether an ID was previously accepted, otherwise record it."""
        if not isinstance(message_id, str) or not message_id:
            # Older/manual messages without an envelope are left compatible.
            return False
        now = time.monotonic()
        cutoff = now - self.ttl_s
        while self._seen:
            _, recorded_at = next(iter(self._seen.items()))
            if recorded_at >= cutoff:
                break
            self._seen.popitem(last=False)
        if message_id in self._seen:
            self._seen.move_to_end(message_id)
            return True
        self._seen[message_id] = now
        while len(self._seen) > self.max_entries:
            self._seen.popitem(last=False)
        return False

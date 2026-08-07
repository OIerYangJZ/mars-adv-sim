"""A minimal logical topology: online agents and their registry links."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TopologySnapshot:
    version: int
    nodes: tuple[str, ...]
    edges: tuple[tuple[str, str], ...]
    task_edges: tuple[tuple[str, str, str], ...] = ()


class TopologyManager:
    """Maintains a deterministic, inspectable topology for the simulator.

    At this stage each online agent has a control-plane edge to ``registry``.
    Task-specific agent-to-agent edges can later be added without changing the
    registry lifecycle logic.
    """

    REGISTRY_NODE = "registry"

    def __init__(self) -> None:
        self._nodes: set[str] = {self.REGISTRY_NODE}
        self._edges: set[tuple[str, str]] = set()
        self._task_edges: dict[str, set[tuple[str, str]]] = {}
        self._version = 0

    @property
    def version(self) -> int:
        return self._version

    def add_agent(self, agent_id: str) -> bool:
        if agent_id in self._nodes:
            return False
        self._nodes.add(agent_id)
        self._edges.add((agent_id, self.REGISTRY_NODE))
        self._version += 1
        return True

    def remove_agent(self, agent_id: str) -> bool:
        if agent_id not in self._nodes:
            return False
        self._nodes.remove(agent_id)
        self._edges = {edge for edge in self._edges if agent_id not in edge}
        changed_tasks = False
        for task_id, edges in list(self._task_edges.items()):
            kept = {edge for edge in edges if agent_id not in edge}
            if kept != edges:
                changed_tasks = True
                if kept:
                    self._task_edges[task_id] = kept
                else:
                    self._task_edges.pop(task_id)
        self._version += 1
        return True

    def add_task_edge(self, task_id: str, src: str, dst: str) -> bool:
        """Add a logical business edge without changing the physical transport."""
        edges = self._task_edges.setdefault(task_id, set())
        edge = (src, dst)
        if edge in edges:
            return False
        edges.add(edge)
        self._version += 1
        return True

    def remove_task_edges(self, task_id: str) -> bool:
        if task_id not in self._task_edges:
            return False
        self._task_edges.pop(task_id)
        self._version += 1
        return True

    def snapshot(self) -> TopologySnapshot:
        return TopologySnapshot(
            version=self._version,
            nodes=tuple(sorted(self._nodes)),
            edges=tuple(sorted(self._edges)),
            task_edges=tuple(
                (task_id, src, dst)
                for task_id, edges in sorted(self._task_edges.items())
                for src, dst in sorted(edges)
            ),
        )

import json
import unittest

from mosaic_omega.scheduler.resource_scheduler import (
    NoSchedulableNodeError,
    ResourceNode,
    ResourceScheduler,
    TaskRequirement,
)


def build_scheduler() -> ResourceScheduler:
    return ResourceScheduler(
        [
            ResourceNode(
                node_id="device_01",
                layer="device",
                skills={"calculation", "control"},
                current_load=0.20,
                latency_ms=5,
                compute_score=25,
                success_rate=0.92,
            ),
            ResourceNode(
                node_id="edge_01",
                layer="edge",
                skills={"calculation", "analysis", "report"},
                current_load=0.35,
                latency_ms=40,
                compute_score=70,
                success_rate=0.95,
            ),
            ResourceNode(
                node_id="cloud_01",
                layer="cloud",
                skills={"planning", "analysis", "report"},
                current_load=0.15,
                latency_ms=160,
                compute_score=100,
                success_rate=0.98,
            ),
        ]
    )


class ResourceSchedulerTests(unittest.TestCase):
    def test_json_input_normalises_single_string_fields(self) -> None:
        scheduler = ResourceScheduler(
            [
                {
                    "node_id": "edge_json",
                    "layer": "edge",
                    "skills": "analysis",
                    "current_load": 0,
                    "latency_ms": 20,
                    "compute_score": 60,
                    "success_rate": 0.9,
                }
            ],
            weights={"latency": "0.3"},
        )

        decision = scheduler.select_node(
            TaskRequirement(
                task_id="task_json",
                description="验证JSON边界",
                required_skill="analysis",
                preferred_layers="edge",
            )
        )

        self.assertEqual(decision.node_id, "edge_json")
        self.assertIn("preferred_layer_bonus", decision.score_breakdown)

    def test_latency_and_skill_constraints_choose_edge(self) -> None:
        scheduler = build_scheduler()
        task = TaskRequirement(
            task_id="task_analysis",
            description="在50毫秒内完成局部分析",
            required_skill="analysis",
            max_latency_ms=50,
        )

        decision = scheduler.select_node(task)

        self.assertEqual(decision.node_id, "edge_01")
        self.assertEqual(decision.layer, "edge")
        self.assertIn("cloud_01", decision.rejected_nodes)
        self.assertIn(
            "latency_limit_exceeded",
            decision.rejected_nodes["cloud_01"],
        )

    def test_local_only_task_stays_on_device(self) -> None:
        scheduler = build_scheduler()

        decision = scheduler.select_node(
            {
                "task_id": "task_control",
                "description": "本地避障控制",
                "required_skill": "control",
                "local_only": True,
                "max_latency_ms": 10,
            }
        )

        self.assertEqual(decision.node_id, "device_01")
        self.assertEqual(decision.layer, "device")

    def test_degraded_route_only_relaxes_latency_and_compute(self) -> None:
        scheduler = build_scheduler()
        task = TaskRequirement(
            task_id="task_global_analysis",
            description="复杂分析",
            required_skill="analysis",
            max_latency_ms=50,
            min_compute_score=90,
            allow_degraded=True,
        )

        decision = scheduler.select_node(task)

        self.assertEqual(decision.node_id, "cloud_01")
        self.assertTrue(decision.degraded)
        self.assertEqual(
            decision.violations,
            ("latency_limit_exceeded",),
        )

    def test_allocation_release_and_migration(self) -> None:
        scheduler = build_scheduler()
        local_task = TaskRequirement(
            task_id="task_calculation",
            description="本地计算",
            required_skill="calculation",
            local_only=True,
            estimated_load=0.2,
        )

        scheduler.allocate_task(local_task)
        self.assertEqual(scheduler.get_assignment("task_calculation"), "device_01")
        self.assertEqual(scheduler.get_node("device_01").current_load, 0.4)
        self.assertEqual(scheduler.release_task("task_calculation"), "device_01")
        self.assertEqual(scheduler.get_node("device_01").current_load, 0.2)

        migration_task = TaskRequirement(
            task_id="task_migrate",
            description="允许降级的分析任务",
            required_skill="analysis",
            max_latency_ms=50,
            allow_degraded=True,
        )
        first_decision = scheduler.allocate_task(migration_task)
        moved_decision = scheduler.migrate_task(
            migration_task,
            failed_node_id=first_decision.node_id,
            mark_failed_offline=True,
        )

        self.assertEqual(first_decision.node_id, "edge_01")
        self.assertEqual(moved_decision.node_id, "cloud_01")
        self.assertTrue(moved_decision.degraded)
        self.assertFalse(scheduler.get_node("edge_01").online)

    def test_no_node_and_heartbeat_timeout(self) -> None:
        scheduler = build_scheduler()
        scheduler.set_online("device_01", False)

        with self.assertRaises(NoSchedulableNodeError) as context:
            scheduler.select_node(
                {
                    "task_id": "task_local",
                    "description": "必须本地执行",
                    "required_skill": "control",
                    "local_only": True,
                }
            )

        self.assertEqual(context.exception.task_id, "task_local")
        self.assertIn("device_01", context.exception.rejected_nodes)

        timed_out = ResourceScheduler(
            [
                ResourceNode(
                    node_id="edge_timeout",
                    layer="edge",
                    skills={"analysis"},
                    current_load=0,
                    latency_ms=20,
                    compute_score=50,
                    success_rate=0.9,
                    last_heartbeat=0,
                )
            ],
            heartbeat_timeout_s=30,
        )

        self.assertEqual(timed_out.expire_stale_nodes(now=31), ["edge_timeout"])
        self.assertFalse(timed_out.get_node("edge_timeout").online)

    def test_ready_dag_tasks_and_json_contract(self) -> None:
        scheduler = build_scheduler()
        plan = [
            {
                "task_id": "task_01",
                "description": "分析输入",
                "required_skill": "analysis",
                "depends_on": [],
                "priority": 8,
            },
            {
                "task_id": "task_02",
                "description": "计算指标",
                "required_skill": "calculation",
                "depends_on": [],
                "priority": 7,
            },
            {
                "task_id": "task_03",
                "description": "撰写报告",
                "required_skill": "report",
                "depends_on": ["task_01", "task_02"],
                "priority": 6,
            },
        ]

        decisions = scheduler.schedule_ready_tasks(plan, [])
        payload = scheduler.schedule_task_payload(plan[0])

        self.assertEqual(set(decisions), {"task_01", "task_02"})
        self.assertEqual(payload["schema_version"], "1.0")
        self.assertEqual(payload["status"], "scheduled")
        json.dumps(payload, ensure_ascii=False)


if __name__ == "__main__":
    unittest.main()

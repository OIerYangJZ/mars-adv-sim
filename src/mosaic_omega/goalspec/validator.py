"""GoalSpec 轻量校验器。"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple
import json
from importlib.resources import files

from jsonschema import Draft202012Validator

REQUIRED_TOP_FIELDS = [
    "main_goal",
    "hard_constraints",
    "soft_preferences",
    "acceptance_conditions",
    "budget",
    "prohibitions",
]
ALLOWED_GOAL_TYPES = {
    "research", "coding", "planning", "robot_task", "analysis", "report_generation", "other"
}
ALLOWED_CHECK_TYPES = {
    "manual_review", "schema_check", "file_check", "content_check", "unit_test", "simulation_check", "metric_check"
}


def validate_goalspec(spec: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errors: List[str] = []

    if not isinstance(spec, dict):
        return False, ["GoalSpec must be a dict/object"]

    unexpected = sorted(set(spec) - set(REQUIRED_TOP_FIELDS))
    if unexpected:
        errors.append(f"unexpected top-level fields: {unexpected}")

    for field in REQUIRED_TOP_FIELDS:
        if field not in spec:
            errors.append(f"missing top-level field: {field}")

    mg = spec.get("main_goal")
    if not isinstance(mg, dict):
        errors.append("main_goal must be an object")
    else:
        for k in ["goal_text", "goal_type", "domain"]:
            if k not in mg:
                errors.append(f"main_goal missing field: {k}")
        if mg.get("goal_type") not in ALLOWED_GOAL_TYPES:
            errors.append(f"invalid goal_type: {mg.get('goal_type')}")
        if not isinstance(mg.get("goal_text"), str) or not mg.get("goal_text", "").strip():
            errors.append("main_goal.goal_text must be a non-empty string")

    if not isinstance(spec.get("hard_constraints"), list):
        errors.append("hard_constraints must be a list")
    else:
        for i, item in enumerate(spec["hard_constraints"]):
            if not isinstance(item, dict):
                errors.append(f"hard_constraints[{i}] must be an object")
                continue
            for k in ["constraint", "checkable", "check_method"]:
                if k not in item:
                    errors.append(f"hard_constraints[{i}] missing field: {k}")
            if "checkable" in item and not isinstance(item["checkable"], bool):
                errors.append(f"hard_constraints[{i}].checkable must be bool")

    if not isinstance(spec.get("soft_preferences"), list):
        errors.append("soft_preferences must be a list")
    else:
        for i, item in enumerate(spec["soft_preferences"]):
            if not isinstance(item, dict):
                errors.append(f"soft_preferences[{i}] must be an object")
                continue
            for k in ["preference", "priority"]:
                if k not in item:
                    errors.append(f"soft_preferences[{i}] missing field: {k}")
            if "priority" in item and not (isinstance(item["priority"], int) and 1 <= item["priority"] <= 5):
                errors.append(f"soft_preferences[{i}].priority must be int in [1, 5]")

    if not isinstance(spec.get("acceptance_conditions"), list):
        errors.append("acceptance_conditions must be a list")
    else:
        for i, item in enumerate(spec["acceptance_conditions"]):
            if not isinstance(item, dict):
                errors.append(f"acceptance_conditions[{i}] must be an object")
                continue
            for k in ["condition", "check_type", "expected_result"]:
                if k not in item:
                    errors.append(f"acceptance_conditions[{i}] missing field: {k}")
            if item.get("check_type") not in ALLOWED_CHECK_TYPES:
                errors.append(f"acceptance_conditions[{i}] invalid check_type: {item.get('check_type')}")

    budget = spec.get("budget")
    if not isinstance(budget, dict):
        errors.append("budget must be an object")
    else:
        for k in ["time_limit", "token_limit", "compute_limit", "cost_limit"]:
            if k not in budget:
                errors.append(f"budget missing field: {k}")
        if budget.get("token_limit") is not None and not isinstance(budget.get("token_limit"), int):
            errors.append("budget.token_limit must be int or null")

    if not isinstance(spec.get("prohibitions"), list):
        errors.append("prohibitions must be a list")
    else:
        for i, item in enumerate(spec["prohibitions"]):
            if not isinstance(item, dict):
                errors.append(f"prohibitions[{i}] must be an object")
                continue
            for k in ["rule", "reason"]:
                if k not in item:
                    errors.append(f"prohibitions[{i}] missing field: {k}")

    return len(errors) == 0, errors


def validate_goalspec_schema(spec: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate GoalSpec against the packaged JSON Schema.

    The six top-level keys are a frozen cross-module contract. Nested objects
    intentionally remain extensible so richer source/confidence/predicate
    metadata can evolve without breaking ToDAG or runtime adapters.
    """
    schema_text = files("mosaic_omega.schemas").joinpath("goalspec.schema.json").read_text(encoding="utf-8")
    schema = json.loads(schema_text)
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(spec), key=lambda e: list(e.absolute_path))
    messages = []
    for error in errors:
        path = ".".join(str(item) for item in error.absolute_path) or "$"
        messages.append(f"{path}: {error.message}")
    return not messages, messages

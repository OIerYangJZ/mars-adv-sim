"""Evidence-gated verifier with a deliberately small executable predicate DSL.

The handbook does not require a theorem prover.  This verifier keeps success
criteria deterministic and auditable:

* ``execution_success`` / ``exit_code==0``
* ``contains:<text>`` -- execution output contains text
* ``file_exists:<relative path>``
* ``file_contains:<relative path>:<text>``
* any other predicate falls back to exact case-insensitive presence in output

Every evidence item is hash-checked before a task may succeed.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote, urlparse

from ..execution_scheduler.models import Evidence, ExecutionResult, TaskNodeView
from .models import PredicateResult, VerificationResult


class VerifierService:
    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace).resolve()

    def _safe_path(self, relative: str) -> Path:
        path = (self.workspace / relative.replace("\\", "/")).resolve()
        if path != self.workspace and self.workspace not in path.parents:
            raise PermissionError("verification path escapes workspace")
        return path

    @staticmethod
    def _evidence_integrity(evidence: Evidence) -> PredicateResult:
        raw = f"{evidence.content}\n{evidence.metadata.get('error', '') or ''}".encode("utf-8")
        digest = hashlib.sha256(raw).hexdigest()
        detail = "content sha256 verified"
        if evidence.uri and evidence.uri.startswith("file:"):
            parsed = urlparse(evidence.uri)
            artifact = Path(unquote(parsed.path))
            if not artifact.is_file():
                return PredicateResult(
                    predicate=f"evidence_hash:{evidence.evidence_id}",
                    passed=False,
                    detail=f"evidence artifact missing: {artifact}",
                )
            digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
            detail = f"artifact sha256 verified: {artifact}"
        passed = evidence.digest == digest
        return PredicateResult(
            predicate=f"evidence_hash:{evidence.evidence_id}",
            passed=passed,
            detail=detail if passed else "evidence digest mismatch",
        )

    def _predicate(self, predicate: str, result: ExecutionResult) -> PredicateResult:
        text = predicate.strip()
        folded = text.casefold()
        if folded in {"execution_success", "exit_code==0", "exit_code = 0"}:
            passed = result.success and result.exit_code in {None, 0}
            return PredicateResult(text, passed, f"exit_code={result.exit_code}")
        if folded.startswith("contains:"):
            needle = text.split(":", 1)[1]
            passed = needle.casefold() in result.output.casefold()
            return PredicateResult(text, passed, "output contains value" if passed else "value missing from output")
        if folded.startswith("file_exists:"):
            relative = text.split(":", 1)[1].strip()
            path = self._safe_path(relative)
            return PredicateResult(text, path.is_file(), str(path))
        if folded.startswith("file_contains:"):
            parts = text.split(":", 2)
            if len(parts) != 3:
                return PredicateResult(text, False, "expected file_contains:<path>:<text>")
            path = self._safe_path(parts[1].strip())
            if not path.is_file():
                return PredicateResult(text, False, f"missing file: {path}")
            content = path.read_text(encoding="utf-8", errors="replace")
            passed = parts[2].casefold() in content.casefold()
            return PredicateResult(text, passed, str(path))
        passed = folded in result.output.casefold()
        return PredicateResult(text, passed, "legacy textual predicate")

    def verify(
        self,
        task: TaskNodeView,
        result: ExecutionResult,
        evidence: Iterable[Evidence],
    ) -> VerificationResult:
        evidence = tuple(evidence)
        checks: list[PredicateResult] = [
            PredicateResult("execution_success", bool(result.success), result.error or ""),
            PredicateResult("evidence_present", bool(evidence), f"count={len(evidence)}"),
        ]
        checks.extend(self._evidence_integrity(item) for item in evidence)
        checks.extend(self._predicate(item, result) for item in task.acceptance_conditions)
        passed = all(item.passed for item in checks)
        # Confidence is deterministic coverage, not an LLM self-report.
        confidence = sum(1.0 for item in checks if item.passed) / max(1, len(checks))
        return VerificationResult(
            target_id=task.task_id,
            passed=passed,
            predicate_results=tuple(checks),
            confidence=confidence,
            evidence_refs=tuple(item.evidence_id for item in evidence),
            risk_level=task.risk,
            action="accept" if passed else "recover_or_safe_stop",
            metadata={"result_success": result.success, "check_count": len(checks)},
        )

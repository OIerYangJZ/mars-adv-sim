"""Dependency-free local HTTP API and dynamic DAG visualization server."""

from __future__ import annotations

import argparse
import json
import mimetypes
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .engine import ToDAGEngine


STATIC_DIR = Path(__file__).with_name("static")
MAX_BODY_BYTES = 1_000_000


class ToDAGRequestHandler(BaseHTTPRequestHandler):
    engine: ToDAGEngine

    def _json(self, payload: Any, status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ValueError("invalid Content-Length") from exc
        if length <= 0 or length > MAX_BODY_BYTES:
            raise ValueError("request body must contain 1 to 1000000 bytes")
        try:
            value = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"request body is not valid UTF-8 JSON: {exc}") from exc
        if not isinstance(value, dict):
            raise ValueError("request body must be a JSON object")
        return value

    def _static(self, relative_path: str) -> None:
        target = (STATIC_DIR / relative_path).resolve()
        if STATIC_DIR.resolve() not in target.parents and target != STATIC_DIR.resolve():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if not target.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        body = target.read_bytes()
        content_type, _ = mimetypes.guess_type(target.name)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", f"{content_type or 'application/octet-stream'}; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/api/dag":
            self._json(self.engine.snapshot())
        elif path == "/api/coordinator-plan":
            try:
                self._json({"tasks": self.engine.coordinator_plan()})
            except RuntimeError as exc:
                self._json({"error": str(exc)}, HTTPStatus.CONFLICT)
        elif path == "/api/ready":
            snapshot = self.engine.snapshot()
            self._json({
                "status": snapshot.get("status"),
                "revision": snapshot.get("revision", 0),
                "ready_task_ids": snapshot.get("ready_task_ids", []),
                "rolling_window_task_ids": snapshot.get("rolling_window_task_ids", []),
                "critical_path": snapshot.get("critical_path", {}),
            })
        elif path in {"/", "/index.html"}:
            self._static("index.html")
        elif path.startswith("/static/"):
            self._static(path.removeprefix("/static/"))
        else:
            self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            body = self._read_json()
            if path == "/api/build":
                self._json(self.engine.build(body), HTTPStatus.CREATED)
                return
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4 and parts[:2] == ["api", "nodes"] and parts[3] == "result":
                status = str(body.pop("status", "completed"))
                evidence = body.get("evidence")
                self._json(self.engine.set_node_result(parts[2], body.get("result"), status, evidence))
                return
            if len(parts) == 4 and parts[:2] == ["api", "nodes"] and parts[3] == "invalidate":
                self._json(self.engine.invalidate_node(parts[2], str(body.get("reason", "external_invalidation"))))
                return
            self.send_error(HTTPStatus.NOT_FOUND)
        except KeyError as exc:
            self._json({"error": f"unknown task: {exc.args[0]}"}, HTTPStatus.NOT_FOUND)
        except (RuntimeError, TypeError, ValueError) as exc:
            self._json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)


    def do_PUT(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path != "/api/specification":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        try:
            self._json(self.engine.update_specification(self._read_json()))
        except (RuntimeError, TypeError, ValueError) as exc:
            self._json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)

    def do_PATCH(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        parts = [unquote(part) for part in path.split("/") if part]
        if len(parts) != 3 or parts[:2] != ["api", "nodes"]:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        try:
            self._json(self.engine.update_node(parts[2], self._read_json()))
        except KeyError as exc:
            self._json({"error": f"unknown task: {exc.args[0]}"}, HTTPStatus.NOT_FOUND)
        except (RuntimeError, TypeError, ValueError) as exc:
            self._json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)

    def log_message(self, format_string: str, *args: Any) -> None:
        print(f"[todag-web] {self.address_string()} {format_string % args}")


def create_server(host: str, port: int, engine: ToDAGEngine | None = None) -> ThreadingHTTPServer:
    active_engine = engine or ToDAGEngine()

    class BoundHandler(ToDAGRequestHandler):
        pass

    BoundHandler.engine = active_engine
    return ThreadingHTTPServer((host, port), BoundHandler)


def run(host: str, port: int, input_path: str | None = None) -> None:
    engine = ToDAGEngine()
    if input_path:
        raw = json.loads(Path(input_path).read_text(encoding="utf-8"))
        engine.build(raw)
    server = create_server(host, port, engine)
    print(f"[todag-web] listening on http://{host}:{server.server_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the ToDAG API and visualization")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8780)
    parser.add_argument("--input", help="optional JSON input document loaded on startup")
    args = parser.parse_args()
    run(args.host, args.port, args.input)


if __name__ == "__main__":
    main()

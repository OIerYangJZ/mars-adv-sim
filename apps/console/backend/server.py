"""Dependency-free read-only HTTP server for the MOSAIC-Ω operator console."""
from __future__ import annotations

import json
import mimetypes
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from mosaic_omega.console_api import ConsoleDataSource


class ConsoleHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], data_source: ConsoleDataSource, frontend_dir: Path) -> None:
        super().__init__(address, ConsoleRequestHandler)
        self.data_source = data_source
        self.frontend_dir = frontend_dir.resolve()


class ConsoleRequestHandler(BaseHTTPRequestHandler):
    server: ConsoleHTTPServer

    def log_message(self, fmt: str, *args: Any) -> None:  # concise operator output
        print(f"[console] {self.address_string()} {fmt % args}")

    def _json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _file(self, path: Path) -> None:
        try:
            resolved = path.resolve()
            resolved.relative_to(self.server.frontend_dir)
        except (ValueError, OSError):
            self.send_error(404)
            return
        if not resolved.is_file():
            self.send_error(404)
            return
        body = resolved.read_bytes()
        content_type = mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    @staticmethod
    def _query(path: str) -> tuple[str, dict[str, list[str]]]:
        parsed = urllib.parse.urlsplit(path)
        return parsed.path, urllib.parse.parse_qs(parsed.query)

    def do_GET(self) -> None:  # noqa: N802
        path, query = self._query(self.path)
        run_id = query.get("run_id", [None])[0]
        if path == "/api/health":
            snapshot = self.server.data_source.snapshot(run_id)
            self._json({"ready": snapshot is not None, "read_only": True})
            return
        if path == "/api/runs":
            self._json(self.server.data_source.runs())
            return
        if path == "/api/snapshot":
            snapshot = self.server.data_source.snapshot(run_id)
            if snapshot is None:
                self._json({"status": "waiting_for_snapshot", "read_only": True}, 404)
            else:
                self._json(snapshot)
            return
        if path == "/api/events":
            self._json(self.server.data_source.events(
                run_id=run_id,
                event_type=query.get("type", [None])[0],
                task_id=query.get("task_id", [None])[0],
                trace_id=query.get("trace_id", [None])[0],
            ))
            return
        if path.startswith("/api/"):
            section = path.removeprefix("/api/").replace("-", "_")
            try:
                payload = self.server.data_source.section(section, run_id)
            except KeyError:
                self._json({"error": "unknown_section", "section": section}, 404)
                return
            if payload is None:
                self._json({"status": "waiting_for_snapshot"}, 404)
            else:
                self._json(payload)
            return

        if path in {"", "/"}:
            self._file(self.server.frontend_dir / "index.html")
            return
        relative = path.lstrip("/")
        self._file(self.server.frontend_dir / relative)

    def do_POST(self) -> None:  # noqa: N802
        self._json({"error": "read_only_console", "message": "Console cannot mutate runtime state."}, 405)

    do_PUT = do_POST  # type: ignore[assignment]
    do_DELETE = do_POST  # type: ignore[assignment]
    do_PATCH = do_POST  # type: ignore[assignment]


def serve(host: str, port: int, *, snapshot_dir: str | Path, frontend_dir: str | Path) -> None:
    source = ConsoleDataSource(snapshot_dir)
    server = ConsoleHTTPServer((host, port), source, Path(frontend_dir))
    print(f"MOSAIC-Ω Console: http://{host}:{port}")
    print(f"Snapshot directory: {Path(snapshot_dir).resolve()}")
    print("Console mode: READ ONLY")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

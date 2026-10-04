"""Test the installed distribution in isolated workspaces and against local HTTP."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tarfile
import threading
import venv
import zipfile
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import fasteners
import pytest


@dataclass
class InstalledCLI:
    executable: Path
    root: Path
    env: dict[str, str]

    def run(self, *args: str, success: bool = True) -> subprocess.CompletedProcess:
        result = subprocess.run(
            [str(self.executable), *args],
            cwd=self.root,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=45,
        )
        assert "Exception in thread" not in result.stderr, result.stderr
        assert "Event loop is closed" not in result.stderr, result.stderr
        with (self.root / "commands.log").open("a") as log:
            log.write(f"{args!r}\nexit={result.returncode}\n{result.stdout}\n{result.stderr}\n")
        if success:
            assert result.returncode == 0, result.stdout + result.stderr
        else:
            assert result.returncode != 0, result.stdout + result.stderr
        return result


@pytest.fixture(scope="session")
def installed_executable(tmp_path_factory):
    repository = Path(__file__).resolve().parents[2]
    build_root = tmp_path_factory.mktemp("installed-distribution")
    (repository / "build").mkdir(exist_ok=True)
    lock = fasteners.InterProcessLock(str(repository / "build" / ".e2e-distribution.lock"))
    assert lock.acquire(blocking=True, timeout=60), "Another distribution build did not finish"
    try:
        build = subprocess.run(
            [
                sys.executable,
                "-m",
                "build",
                "--no-isolation",
                "--outdir",
                str(build_root),
                str(repository),
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
    finally:
        lock.release()
    assert build.returncode == 0, build.stdout + build.stderr
    wheels = list(build_root.glob("*.whl"))
    assert len(wheels) == 1
    sources = list(build_root.glob("*.tar.gz"))
    assert len(sources) == 1
    with zipfile.ZipFile(wheels[0]) as archive:
        wheel_files = archive.namelist()
    with tarfile.open(sources[0]) as archive:
        source_files = archive.getnames()
    for files in (wheel_files, source_files):
        assert any(path.endswith("/alembic.ini") for path in files)
        assert any(path.endswith("/alembic/env.py") for path in files)
        assert any(
            path.endswith("/alembic/versions/0005_add_ingestion_jobs_table.py") for path in files
        )
    metadata = subprocess.run(
        [sys.executable, "-m", "twine", "check", "--strict", str(wheels[0]), str(sources[0])],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert metadata.returncode == 0, metadata.stdout + metadata.stderr
    environment = build_root / "venv"
    venv.create(environment, system_site_packages=True)
    executable = environment / "bin" / "python"
    subprocess.run(
        [
            str(executable),
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--ignore-installed",
            str(wheels[0]),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    check = subprocess.run(
        [str(executable), "-c", "import marketpipe; print(marketpipe.__file__)"],
        cwd=build_root,
        env={"PATH": os.defpath},
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert str(environment) in check.stdout, check.stdout
    return environment / "bin" / "marketpipe"


@pytest.fixture
def cli(installed_executable, tmp_path):
    environment = {
        "PATH": str(installed_executable.parent) + os.pathsep + os.defpath,
        "PYTHONHASHSEED": "0",
        "NO_COLOR": "1",
        "MARKETPIPE_RAW_ROOT": str(tmp_path / "data" / "raw"),
        "MARKETPIPE_AGG_ROOT": str(tmp_path / "data" / "agg"),
        "MARKETPIPE_DB_PATH": str(tmp_path / "data" / "db" / "core.db"),
        "MARKETPIPE_CHECKPOINT_DB_PATH": str(tmp_path / "data" / "db" / "core.db"),
        "MARKETPIPE_INGESTION_DB_PATH": str(tmp_path / "data" / "ingestion_jobs.db"),
        "MARKETPIPE_METRICS_DB_PATH": str(tmp_path / "data" / "metrics.db"),
    }
    return InstalledCLI(installed_executable, tmp_path, environment)


@pytest.fixture
def http_server():
    """A scripted real HTTP boundary; unexpected requests receive a failing response."""
    requests = []
    responses = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlsplit(self.path)
            requests.append((parsed.path, parse_qs(parsed.query)))
            if responses:
                response = responses.pop(0)
                if callable(response):
                    response = response(parsed, parse_qs(parsed.query))
                status, headers, body = response
            else:
                status, headers, body = 500, {}, {"error": "unexpected request"}
            encoded = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            try:
                self.wfile.write(encoded)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", responses, requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()

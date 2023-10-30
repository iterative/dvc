import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from contextlib import contextmanager
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from threading import Thread
from typing import Dict, Iterator

import psutil
import pytest

from dvc import version_tuple
from dvc.daemon import _get_dvc_args
from dvc.env import (
    DVC_ANALYTICS_ENDPOINT,
    DVC_DAEMON_LOGFILE,
    DVC_NO_ANALYTICS,
    DVC_UPDATER_ENDPOINT,
)
from dvc.updater import Updater

version = ".".join(map(str, version_tuple[:3]))
UPDATER_INFO = {
    "version": version,
    "packages": {
        "linux": {
            "deb": f"https://dvc.org/download/linux-deb/dvc-{version}",
            "rpm": f"https://dvc.org/download/linux-rpm/dvc-{version}",
        },
        "windows": {"exe": f"https://dvc.org/download/win/dvc-{version}"},
        "osx": {"pkg": f"https://dvc.org/download/osx/dvc-{version}"},
    },
}

UPDATER_INFO_STR = json.dumps(UPDATER_INFO).encode("utf8")


def make_request_handler():
    class RequestHandler(SimpleHTTPRequestHandler):
        # save requests count for each method
        hits: Dict[str, int] = defaultdict(int)

        def log_message(self, format, *args) -> None:  # noqa: A002
            super().log_message(format, *args)
            if length := self.headers.get("Content-Length"):
                data = self.rfile.read(int(length)).decode("utf8")
                sys.stderr.write(f"{data}\n")

        def do_POST(self):  # noqa: N802
            # analytics endpoint
            self.hits["POST"] += 1
            self.send_response(200)
            super().end_headers()

        def do_GET(self):  # noqa: N802
            # updater endpoint
            self.hits["GET"] += 1
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(UPDATER_INFO_STR)

    return RequestHandler


@contextmanager
def make_server(port: int = 0) -> Iterator["HTTPServer"]:
    with HTTPServer(("localhost", port), make_request_handler()) as httpd:
        yield httpd


@pytest.fixture
def server():
    with make_server() as httpd:
        thread = Thread(target=httpd.serve_forever)
        thread.daemon = True
        thread.start()
        yield httpd


def test_analytics(tmp_path, server):
    addr = server.server_address
    logfile = tmp_path / "logfile"

    env = {
        **os.environ,
        DVC_DAEMON_LOGFILE: str(logfile),
        DVC_ANALYTICS_ENDPOINT: "http://{}:{}".format(*addr),
    }
    env.pop("DVC_TEST", None)
    env.pop("DVC_NO_ANALYTICS", None)

    output = subprocess.check_output(
        [*_get_dvc_args(), "config", "-l", "-vv"],
        env=env,
        text=True,
    )

    match = re.search(r".*Saving analytics report to (.*)", output, flags=re.M)
    assert match, "no match for the report file"
    report_file = match.group(1).strip()

    match = re.search(r".*Spawned .*analytics.* with pid (.*)", output, flags=re.M)
    assert match, "no match for the pid"
    pid = int(match.group(1).strip())

    psutil.Process(pid).wait(timeout=10)
    assert not os.path.exists(report_file)
    assert f"Process {pid} exiting with 0" in logfile.read_text(encoding="utf8")
    assert server.RequestHandlerClass.hits == {"POST": 1}


def test_updater(tmp_dir, dvc, server):
    addr = server.server_address
    logfile = tmp_dir / "logfile"

    env = {
        **os.environ,
        DVC_DAEMON_LOGFILE: str(logfile),
        DVC_UPDATER_ENDPOINT: "http://{}:{}".format(*addr),
        # prevent running analytics daemon
        DVC_NO_ANALYTICS: "true",
    }
    env.pop("DVC_TEST", None)
    env.pop("CI", None)

    output = subprocess.check_output(
        [*_get_dvc_args(), "version", "-vv"],
        env=env,
        text=True,
    )

    match = re.search(r".*Spawned .*updater.* with pid (.*)", output, flags=re.M)
    assert match, "no match for the pid"
    pid = int(match.group(1).strip())

    psutil.Process(pid).wait(timeout=10)
    assert f"Process {pid} exiting with 0" in logfile.read_text(encoding="utf8")
    assert server.RequestHandlerClass.hits == {"GET": 1}
    # check that the file is saved correctly
    updater_file = Path(dvc.tmp_dir) / Updater.UPDATER_FILE
    assert json.loads(updater_file.read_text(encoding="utf8")) == UPDATER_INFO


if __name__ == "__main__":
    # python -m tests.func.test_daemon [<port>]
    port = int(sys.argv[1]) if len(sys.argv) >= 2 else 0
    with make_server(port) as httpd:
        print(  # noqa:  T201
            "Running server on http://{}:{}".format(*httpd.server_address)
        )
        httpd.serve_forever()

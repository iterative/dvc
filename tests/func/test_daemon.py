import json
import os
import re
import subprocess
import time
from collections import defaultdict
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from threading import Thread
from typing import Dict

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


@pytest.fixture
def server():
    with HTTPServer(("localhost", 0), make_request_handler()) as httpd:
        thread = Thread(target=httpd.serve_forever)
        thread.daemon = True
        thread.start()
        yield httpd


def retry_until(pred, timeout):
    timeout_ns = timeout * 1e9
    start = time.perf_counter_ns()
    while time.perf_counter_ns() - start < timeout_ns:
        if pred():
            return
        time.sleep(0.01)
    raise RuntimeError(f"timed out after {timeout}s")


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

    pattern = r".*Saving analytics report to (.*)"
    for line in output.splitlines():
        if match := re.search(pattern, line):
            report_file = match.group(1).strip()
            break
    else:
        raise AssertionError("no match for the report file")

    # wait until the file disappears
    retry_until(lambda: not os.path.exists(report_file), 10)
    # wait till the daemon exits
    retry_until(lambda: "exiting with 0" in logfile.read_text(encoding="utf8"), 5)
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

    subprocess.check_output(
        [*_get_dvc_args(), "version", "-vv"],
        env=env,
        text=True,
    )

    updater_file = Path(dvc.tmp_dir) / Updater.UPDATER_FILE

    # wait until the updater file appears
    retry_until(updater_file.is_file, 10)
    # wait till the daemon exits
    retry_until(lambda: "exiting with 0" in logfile.read_text(encoding="utf8"), 5)
    assert server.RequestHandlerClass.hits == {"GET": 1}
    # check that the file is saved correctly
    assert json.loads(updater_file.read_text(encoding="utf8")) == UPDATER_INFO

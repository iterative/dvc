import json
import os
import sys
from contextlib import suppress

import pytest

from dvc import env
from dvc.stage import PipelineStage
from dvc.testing.fixtures import *  # noqa: F403

from .dir_helpers import *  # noqa: F403
from .remotes import *  # noqa: F403
from .scripts import *  # noqa: F403

# Prevent updater and analytics from running their processes
os.environ["DVC_TEST"] = "true"
# Ensure progress output even when not outputting to raw sys.stderr console
os.environ["DVC_IGNORE_ISATTY"] = "true"
# Disable system git config
os.environ["GIT_CONFIG_NOSYSTEM"] = "1"

REMOTES = {
    # remote: enabled_by_default?
    "azure": False,
    "gdrive": False,
    "gs": False,
    "hdfs": True,
    "real_hdfs": False,
    "http": True,
    "oss": False,
    "s3": False,
    "ssh": False,
    "webdav": True,
}


@pytest.fixture(autouse=True)
def reset_loglevel(request, caplog):
    """
    Use it to ensure log level at the start of each test
    regardless of dvc.logger.setup(), Repo configs or whatever.
    """
    ini_opt = None
    with suppress(ValueError):
        ini_opt = request.config.getini("log_level")

    level = request.config.getoption("--log-level") or ini_opt
    if level:
        with caplog.at_level(level.upper(), logger="dvc"):
            yield
    else:
        yield


@pytest.fixture(autouse=True)
def enable_ui():
    from dvc.ui import ui

    ui.enable()


@pytest.fixture(autouse=True)
def clean_repos():
    from dvc.repo.open_repo import clean_repos

    clean_repos()


def _get_opt(remote_name, action):
    return f"--{action}-{remote_name}"


def pytest_addoption(parser):
    """Adds remote-related flags to selectively disable/enable for tests
    Eg: If some remotes, eg: ssh is enabled to be tested for by default
    (see above `REMOTES`), then, `--disable-ssh` flag is added. If remotes
    like `hdfs` are disabled by default, `--enable-hdfs` is added to make them
    run.

    You can also make everything run-by-default with `--all` flag, which takes
    precedence on all previous `--enable-*`/`--disable-*` flags.
    """
    parser.addoption(
        "--all",
        action="store_true",
        default=False,
        help="Test all of the remotes, unless other flags also supplied",
    )
    for remote_name in REMOTES:
        for action in ("enable", "disable"):
            opt = _get_opt(remote_name, action)
            parser.addoption(
                opt,
                action="store_true",
                default=None,
                help=f"{action} tests for {remote_name}",
            )


class DVCTestConfig:
    def __init__(self):
        self.enabled_remotes = set()

    def requires(self, remote_name):
        if remote_name not in REMOTES or remote_name in self.enabled_remotes:
            return

        pytest.skip(f"{remote_name} tests not enabled through CLI")

    def apply_marker(self, marker):
        self.requires(marker.name)


def pytest_runtest_setup(item):
    # Apply test markers to skip tests selectively
    # NOTE: this only works on individual tests,
    # for fixture, use `test_config` fixture and
    # run `test_config.requires(remote_name)`.
    for marker in item.iter_markers():
        item.config.dvc_config.apply_marker(marker)

    if "CI" in os.environ and item.get_closest_marker("needs_internet") is not None:
        # remotes that need internet connection might be flaky,
        # so we rerun them in case it fails.
        item.add_marker(pytest.mark.flaky(reruns=5))


@pytest.fixture(scope="session")
def test_config(request):
    return request.config.dvc_config


def pytest_configure(config):
    config.dvc_config = DVCTestConfig()

    for remote_name in REMOTES:
        config.addinivalue_line(
            "markers", f"{remote_name}: mark test as requiring {remote_name}"
        )

    enabled_remotes = config.dvc_config.enabled_remotes
    if config.getoption("--all"):
        enabled_remotes.update(REMOTES)
    else:
        default_enabled = {k for k, v in REMOTES.items() if v}
        enabled_remotes.update(default_enabled)

    for remote_name in REMOTES:
        enabled_opt = _get_opt(remote_name, "enable")
        disabled_opt = _get_opt(remote_name, "disable")

        enabled = config.getoption(enabled_opt)
        disabled = config.getoption(disabled_opt)
        if disabled and enabled:
            continue  # default behavior if both flags are supplied

        if disabled:
            enabled_remotes.discard(remote_name)
        if enabled:
            enabled_remotes.add(remote_name)


@pytest.fixture
def custom_template(tmp_dir, dvc):
    from dvc_render.vega_templates import SimpleLinearTemplate

    template = tmp_dir / "custom_template.json"
    template.write_text(json.dumps(SimpleLinearTemplate.DEFAULT_CONTENT))
    return template


@pytest.fixture(autouse=True)
def mocked_webbrowser_open(mocker):
    mocker.patch("webbrowser.open")


@pytest.fixture(scope="session", autouse=True)
def isolate(tmp_path_factory):
    path = tmp_path_factory.mktemp("mock")
    home_dir = path / "home"
    home_dir.mkdir()

    monkeypatch = pytest.MonkeyPatch()
    if sys.platform == "win32":
        home_drive, home_path = os.path.splitdrive(home_dir)
        monkeypatch.setenv("USERPROFILE", str(home_dir))
        monkeypatch.setenv("HOMEDRIVE", home_drive)
        monkeypatch.setenv("HOMEPATH", home_path)

        for env_var, sub_path in (("APPDATA", "Roaming"), ("LOCALAPPDATA", "Local")):
            path = home_dir / "AppData" / sub_path
            path.mkdir(parents=True)
            monkeypatch.setenv(env_var, os.fspath(path))
    else:
        monkeypatch.setenv("HOME", str(home_dir))

    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    contents = b"""
[user]
name=DVC Tester
email=dvctester@example.com
[init]
defaultBranch=master
"""
    (home_dir / ".gitconfig").write_bytes(contents)

    import pygit2

    pygit2.settings.search_path[pygit2.GIT_CONFIG_LEVEL_GLOBAL] = str(home_dir)

    monkeypatch.setenv(env.DVC_SYSTEM_CONFIG_DIR, os.fspath(path / "system"))
    monkeypatch.setenv(env.DVC_GLOBAL_CONFIG_DIR, os.fspath(path / "global"))
    monkeypatch.setenv(env.DVC_SITE_CACHE_DIR, os.fspath(path / "site_cache_dir"))

    yield

    monkeypatch.undo()


@pytest.fixture
def run_copy_metrics(tmp_dir, copy_script):
    def run(
        file1,
        file2,
        commit=None,
        tag=None,
        single_stage=True,
        name=None,
        **kwargs,
    ):
        if name:
            single_stage = False

        stage = tmp_dir.dvc.run(
            cmd=f"python copy.py {file1} {file2}",
            deps=[file1],
            single_stage=single_stage,
            name=name,
            **kwargs,
        )

        if hasattr(tmp_dir.dvc, "scm"):
            files = [stage.path]
            if isinstance(stage, PipelineStage):
                files += [stage.dvcfile._lockfile.path]
            files += [out.fs_path for out in stage.outs if not out.use_cache]
            tmp_dir.dvc.scm.add(files)
            if commit:
                tmp_dir.dvc.scm.commit(commit)
            if tag:
                tmp_dir.dvc.scm.tag(tag)
        return stage

    return run

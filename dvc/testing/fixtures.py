import os
import pathlib
import subprocess  # nosec B404
from typing import Dict, Tuple

import pytest

from .scripts import copy_script

# pylint: disable=redefined-outer-name,unused-argument

__all__ = [
    "make_tmp_dir",
    "tmp_dir",
    "scm",
    "dvc",
    "make_cloud",
    "make_cloud_version_aware",
    "make_local",
    "cloud",
    "local_cloud",
    "make_remote",
    "make_remote_version_aware",
    "make_remote_worktree",
    "remote",
    "remote_version_aware",
    "remote_worktree",
    "local_remote",
    "workspace",
    "make_workspace",
    "local_workspace",
    "docker_compose_project_name",
    "docker_services",
    "copy_script",
    "run_copy",
]

CACHE: Dict[Tuple[bool, bool, bool], str] = {}


@pytest.fixture(scope="session")
def make_tmp_dir(tmp_path_factory, request, worker_id):
    def make(name, *, scm=False, dvc=False, subdir=False):  # pylint: disable=W0621
        from shutil import copytree, ignore_patterns

        from dvc.repo import Repo
        from dvc.scm import Git

        from .tmp_dir import TmpDir

        cache = CACHE.get((scm, dvc, subdir))
        if not cache:
            cache_dir = tmp_path_factory.mktemp("dvc-test-cache" + worker_id)
            TmpDir(cache_dir).init(scm=scm, dvc=dvc, subdir=subdir)
            CACHE[(scm, dvc, subdir)] = cache = os.fspath(cache_dir)

        assert cache
        path = tmp_path_factory.mktemp(name) if isinstance(name, str) else name

        # ignore sqlite files from .dvc/tmp. We might not be closing the cache
        # connection resulting in PermissionErrors in Windows.
        ignore = ignore_patterns("cache.db*")
        copytree(cache, path, dirs_exist_ok=True, ignore=ignore)
        new_dir = TmpDir(path)
        str_path = os.fspath(new_dir)
        if dvc:
            new_dir.dvc = Repo(str_path)
        if scm:
            new_dir.scm = new_dir.dvc.scm if hasattr(new_dir, "dvc") else Git(str_path)
        request.addfinalizer(new_dir.close)
        return new_dir

    return make


@pytest.fixture
def tmp_dir(tmp_path, make_tmp_dir, request, monkeypatch):
    monkeypatch.chdir(tmp_path)
    fixtures = request.fixturenames
    return make_tmp_dir(tmp_path, scm="scm" in fixtures, dvc="dvc" in fixtures)


@pytest.fixture
def scm(tmp_dir):
    return tmp_dir.scm


@pytest.fixture
def dvc(tmp_dir):
    with tmp_dir.dvc as _dvc:
        yield _dvc


@pytest.fixture
def make_local(make_tmp_dir):
    def _make_local():
        return make_tmp_dir("local-cloud")

    return _make_local


@pytest.fixture
def make_cloud(request):
    def _make_cloud(typ):
        return request.getfixturevalue(f"make_{typ}")()

    return _make_cloud


@pytest.fixture
def make_cloud_version_aware(request):
    def _make_cloud(typ):
        return request.getfixturevalue(f"make_{typ}_version_aware")()

    return _make_cloud


@pytest.fixture
def cloud(make_cloud, request):
    typ = getattr(request, "param", "local")
    return make_cloud(typ)


@pytest.fixture
def local_cloud(make_cloud):
    return make_cloud("local")


@pytest.fixture
def make_remote(tmp_dir, dvc, make_cloud):  # noqa: ARG001
    def _make_remote(name, typ="local", **kwargs):
        cloud = make_cloud(typ)  # pylint: disable=W0621
        tmp_dir.add_remote(name=name, config=cloud.config, **kwargs)
        return cloud

    return _make_remote


@pytest.fixture
def make_remote_version_aware(tmp_dir, dvc, make_cloud_version_aware):  # noqa: ARG001
    def _make_remote(name, typ="local", **kwargs):
        cloud = make_cloud_version_aware(typ)  # pylint: disable=W0621
        config = dict(cloud.config)
        config["version_aware"] = True
        tmp_dir.add_remote(name=name, config=config, **kwargs)
        return cloud

    return _make_remote


@pytest.fixture
def make_remote_worktree(tmp_dir, dvc, make_cloud_version_aware):  # noqa: ARG001
    def _make_remote(name, typ="local", **kwargs):
        cloud = make_cloud_version_aware(typ)  # pylint: disable=W0621
        config = dict(cloud.config)
        config["worktree"] = True
        tmp_dir.add_remote(name=name, config=config, **kwargs)
        return cloud

    return _make_remote


@pytest.fixture
def remote(make_remote, request):
    typ = getattr(request, "param", "local")
    return make_remote("upstream", typ=typ)


@pytest.fixture
def remote_version_aware(make_remote_version_aware, request):
    typ = getattr(request, "param", "local")
    return make_remote_version_aware("upstream", typ=typ)


@pytest.fixture
def remote_worktree(make_remote_worktree, request):
    typ = getattr(request, "param", "local")
    return make_remote_worktree("upstream", typ=typ)


@pytest.fixture
def local_remote(make_remote):
    return make_remote("upstream", typ="local")


@pytest.fixture
def make_workspace(tmp_dir, dvc, make_cloud):
    def _make_workspace(name, typ="local"):
        from dvc.cachemgr import CacheManager

        cloud = make_cloud(typ)  # pylint: disable=W0621

        tmp_dir.add_remote(name=name, config=cloud.config, default=False)
        tmp_dir.add_remote(
            name=f"{name}-cache", url="remote://workspace/cache", default=False
        )

        scheme = getattr(cloud, "scheme", "local")
        if scheme != "http":
            with dvc.config.edit() as conf:
                conf["cache"][scheme] = f"{name}-cache"

            dvc.cache = CacheManager(dvc)

        return cloud

    return _make_workspace


@pytest.fixture
def workspace(make_workspace, request):
    typ = getattr(request, "param", "local")
    return make_workspace("workspace", typ=typ)


@pytest.fixture
def local_workspace(make_workspace):
    return make_workspace("workspace", typ="local")


@pytest.fixture(scope="session")
def docker_compose_project_name():
    return "pytest-dvc-test"


@pytest.fixture(scope="session")
def docker_services(
    tmp_path_factory,
    docker_compose_command,
    docker_compose_file,
    docker_compose_project_name,
    docker_setup,
):
    from filelock import FileLock
    from pytest_docker.plugin import DockerComposeExecutor, Services

    if os.environ.get("CI") and os.name == "nt":
        pytest.skip("disabled for Windows on CI")

    try:
        subprocess.check_output(  # nosec B607, B602,
            "docker ps", stderr=subprocess.STDOUT, shell=True  # noqa: S602, S607
        )
    except subprocess.CalledProcessError as err:
        out = (err.output or b"").decode("utf-8")
        pytest.skip(f"docker is not installed or the daemon is not running: {out}")

    try:
        cmd = "docker-compose version"
        subprocess.check_output(
            cmd, stderr=subprocess.STDOUT, shell=True  # nosec B602 # noqa: S602
        )
    except subprocess.CalledProcessError as err:
        out = (err.output or b"").decode("utf-8")
        pytest.skip(f"docker-compose is not installed: {out}")

    executor = DockerComposeExecutor(
        docker_compose_command, docker_compose_file, docker_compose_project_name
    )

    # making sure we don't accidentally launch docker-compose in parallel,
    # as it might result in network conflicts. Inspired by:
    # https://github.com/pytest-dev/pytest-xdist#making-session-scoped-fixtures-execute-only-once
    lockfile = tmp_path_factory.getbasetemp().parent / "docker-compose.lock"
    with FileLock(os.fspath(lockfile)):
        executor.execute(docker_setup)
        # note: we are not tearing down the containers here
        return Services(executor)


@pytest.fixture
def run_copy(tmp_dir, copy_script, dvc):  # noqa: ARG001
    def run_copy(src, dst, **run_kwargs):
        wdir = pathlib.Path(run_kwargs.get("wdir", "."))
        wdir = pathlib.Path("../" * len(wdir.parts))
        script_path = wdir / "copy.py"

        return dvc.run(
            cmd=f"python {script_path} {src} {dst}",
            outs=[dst],
            deps=[src, f"{script_path}"],
            **run_kwargs,
        )

    return run_copy

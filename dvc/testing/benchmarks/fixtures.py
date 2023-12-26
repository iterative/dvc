import os
import shutil
from pathlib import Path
from subprocess import check_output
from typing import Dict, Optional

import pytest
from dulwich.porcelain import clone
from funcy import first
from packaging import version

from dvc.types import StrPath


@pytest.fixture(scope="session")
def bench_config(request):
    return request.config.bench_config


class VirtualEnv:
    def __init__(self, path: StrPath) -> None:
        self.path = Path(path)
        self.bin = self.path / ("Scripts" if os.name == "nt" else "bin")

    def create(self) -> None:
        import virtualenv

        virtualenv.cli_run([os.fspath(self.path)])

    def run(self, cmd: str, *args: str, env: Optional[Dict[str, str]] = None) -> None:
        exe = self.which(cmd)
        check_output([exe, *args], env=env)  # noqa: S603

    def which(self, cmd: str) -> str:
        assert self.bin.exists()
        return shutil.which(cmd, path=self.bin) or cmd


@pytest.fixture(scope="session")
def make_dvc_venv(tmp_path_factory):
    def _make_dvc_venv(name):
        name = _sanitize_venv_name(name)
        venv_dir = tmp_path_factory.mktemp(f"dvc-venv-{name}")
        venv = VirtualEnv(venv_dir)
        venv.create()
        return venv

    return _make_dvc_venv


def _sanitize_venv_name(name):
    return name.replace("/", "-").replace("\\", "-")


@pytest.fixture(scope="session")
def dvc_venvs():
    return {}


@pytest.fixture(scope="session")
def dvc_git_repo(tmp_path_factory, bench_config):
    url = bench_config.dvc_git_repo

    if os.path.isdir(url):
        return url

    tmp_path = tmp_path_factory.mktemp("dvc-git-repo")
    clone(url, os.fspath(tmp_path))

    return tmp_path


@pytest.fixture(scope="session")
def dvc_bench_git_repo(tmp_path_factory, bench_config):
    url = bench_config.dvc_bench_git_repo

    if os.path.isdir(url):
        return Path(url)

    tmp_path = tmp_path_factory.mktemp("dvc-bench-git-repo")
    clone(url, os.fspath(tmp_path))

    return tmp_path


@pytest.fixture(scope="session")
def make_dvc_bin(
    dvc_rev,
    dvc_venvs,
    make_dvc_venv,
    dvc_git_repo,
    bench_config,
    request,
):
    if dvc_rev:
        venv = dvc_venvs.get(dvc_rev)
        if not venv:
            venv = make_dvc_venv(dvc_rev)
            venv.run("pip", "install", "-U", "pip")
            if bench_config.dvc_install_deps:
                egg = f"dvc[{bench_config.dvc_install_deps}]"
            else:
                egg = "dvc"
            venv.run("pip", "install", f"git+file://{dvc_git_repo}@{dvc_rev}#egg={egg}")
            if dvc_rev in ["2.18.1", "2.11.0", "2.6.3"]:
                venv.run("pip", "install", "fsspec==2022.11.0")
            dvc_venvs[dvc_rev] = venv
        dvc_bin = venv.which("dvc")
    else:
        dvc_bin = bench_config.dvc_bin

    def _dvc_bin(*args):
        return check_output([dvc_bin, *args], text=True)  # noqa: S603

    _dvc_bin.version = parse_tuple(_dvc_bin("--version"))  # type: ignore[attr-defined]
    return _dvc_bin


def parse_tuple(version_string):
    from packaging.version import Version

    parsed = version.parse(version_string)
    assert isinstance(parsed, Version)
    return (parsed.major, parsed.minor, parsed.micro)


@pytest.fixture
def dvc_bin(request, make_dvc_bin):
    if marker := request.node.get_closest_marker("requires"):
        minversion = marker.kwargs.get("minversion") or first(marker.args)
        assert minversion, (
            "'minversion' needs to be specified as"
            " a positional or a keyword argument"
        )
        reason = marker.kwargs.get("reason", "")
        if isinstance(minversion, str):
            minversion = parse_tuple(minversion)
        if make_dvc_bin.version < minversion:
            version_repr = ".".join(map(str, minversion))
            pytest.skip(f"requires dvc>={version_repr}: {reason}")
    return make_dvc_bin


@pytest.fixture
def make_bench(request):
    def _make_bench(name):
        import pytest_benchmark.plugin

        # hack from https://github.com/ionelmc/pytest-benchmark/issues/166
        bench = pytest_benchmark.plugin.benchmark.__pytest_wrapped__.obj(request)

        suffix = f"-{name}"

        def add_suffix(_name):
            start, sep, end = _name.partition("[")
            return start + suffix + sep + end

        bench.name = add_suffix(bench.name)
        bench.fullname = add_suffix(bench.fullname)

        return bench

    return _make_bench


@pytest.fixture
def bench_dvc(dvc_bin, make_bench):
    def _bench_dvc(*args, **kwargs):
        name = kwargs.pop("name", None)
        name = f"-{name}" if name else ""
        bench = make_bench(args[0] + name)
        return bench.pedantic(dvc_bin, args=args, **kwargs)

    return _bench_dvc


def _pull(repo, *args):
    from dvc.exceptions import CheckoutError, DownloadError

    while True:
        try:
            return repo.pull(*args)
        except (CheckoutError, DownloadError):
            pass


@pytest.fixture
def make_dataset(request, bench_config, tmp_dir, dvc_bench_git_repo):
    def _make_dataset(
        dvcfile=False, files=True, cache=False, commit=False, remote=False
    ):
        from dvc.repo import Repo

        path = tmp_dir / "dataset"
        root = dvc_bench_git_repo
        src = root / "data" / bench_config.dataset / "dataset"
        src_dvc = src.with_suffix(".dvc")

        dvc = Repo(root)

        _pull(dvc, [str(src_dvc)])
        if files:
            shutil.copytree(src, path)
        if dvcfile:
            shutil.copy(src.with_suffix(".dvc"), path.with_suffix(".dvc"))
        if cache:
            shutil.copytree(root / ".dvc" / "cache", tmp_dir / ".dvc" / "cache")
        if remote:
            assert dvcfile
            assert not cache
            assert tmp_dir.dvc
            # FIXME temporary hack, we should try to push from home repo
            # directly to this remote instead
            shutil.copytree(root / ".dvc" / "cache", tmp_dir / ".dvc" / "cache")
            tmp_dir.dvc.push([str(path.with_suffix(".dvc").relative_to(tmp_dir))])
            shutil.rmtree(tmp_dir / ".dvc" / "cache")
        if commit:
            assert dvcfile
            assert tmp_dir.scm
            tmp_dir.scm.add([str(path.with_suffix(".dvc").relative_to(tmp_dir))])
            tmp_dir.scm.commit("add dataset")
        return path

    return _make_dataset


@pytest.fixture
def dataset(make_dataset):
    return make_dataset(dvcfile=False, files=True, cache=False)


@pytest.fixture
def remote_dataset():
    pytest.skip("fixme")


@pytest.fixture
def make_project(tmp_path_factory):
    def _make_project(url, rev=None):
        path = os.fspath(tmp_path_factory.mktemp("dvc-project"))

        if rev:
            rev = rev.encode("ascii")

        clone(url, path, branch=rev)
        return path

    return _make_project


@pytest.fixture
def project(bench_config, monkeypatch, make_project):
    rev = bench_config.project_rev
    url = bench_config.project_git_repo

    if os.path.isdir(url):
        path = url
        assert not rev
    else:
        path = make_project(url, rev=rev)

    monkeypatch.chdir(path)

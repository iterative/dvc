import inspect
import os
import shutil
import sys
from pathlib import Path
from subprocess import check_call, check_output
from typing import Optional

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
        check_call([sys.executable, "-m", "uv", "venv", self.path])  # noqa:S603

    def install(self, *packages: str) -> None:
        check_call([sys.executable, "-m", "uv", "pip", "install", *packages])  # noqa: S603

    def run(self, cmd: str, *args: str, env: Optional[dict[str, str]] = None) -> None:
        exe = self.which(cmd)
        check_call([exe, *args], env=env)  # noqa: S603

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
def dvc_repo(tmp_path_factory, bench_config):
    url = bench_config.dvc_repo

    if os.path.isdir(url):
        return url

    tmp_path = tmp_path_factory.mktemp("dvc-git-repo")
    clone(url, os.fspath(tmp_path))

    return tmp_path


@pytest.fixture(scope="session")
def dvc_bench_repo(tmp_path_factory, bench_config):
    url = bench_config.dvc_bench_repo
    if url is None:
        pytest.skip(
            "--dvc-bench-repo is not set, "
            "clone https://github.com/iterative/dvc-bench repository and set its path"
        )

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
    dvc_repo,
    bench_config,
    request,
):
    if dvc_rev:
        venv: VirtualEnv = dvc_venvs.get(dvc_rev)
        if not venv:
            venv = make_dvc_venv(dvc_rev)
            if bench_config.dvc_install_deps:
                pkg = f"dvc[{bench_config.dvc_install_deps}]"
            else:
                pkg = "dvc"
            packages = [f"{pkg} @ git+file://{dvc_repo}@{dvc_rev}"]
            try:
                if version.Version(dvc_rev) < version.Version("3.50.3"):
                    packages.append("pygit2==1.14.1")
            except version.InvalidVersion:
                pass
            venv.install(*packages)

            dvc_venvs[dvc_rev] = venv
        dvc_bin = venv.which("dvc")
    else:
        dvc_bin = bench_config.dvc_bin

    def _dvc_bin(*args):
        check_call([dvc_bin, *args])  # noqa: S603

    _dvc_bin.version = check_output([dvc_bin, "--version"], text=True)  # type: ignore[attr-defined]  # noqa: S603
    return _dvc_bin


@pytest.fixture
def dvc_bin(request, make_dvc_bin):
    if marker := request.node.get_closest_marker("requires"):
        from packaging.specifiers import SpecifierSet
        from packaging.version import Version, parse

        spec = first(marker.args)
        assert spec is not None
        spec = SpecifierSet(spec) if isinstance(spec, str) else spec
        reason = marker.kwargs["reason"]
        dvc_version = make_dvc_bin.version
        version = Version(parse(dvc_version).base_version)
        if version not in spec:
            pytest.skip(
                f"Version {dvc_version} does not satisfy requirement {spec!r}: {reason}"
            )
    return make_dvc_bin


@pytest.fixture
def make_bench(request):
    def _make_bench(name):
        import pytest_benchmark.plugin

        # hack from https://github.com/ionelmc/pytest-benchmark/issues/166
        fixture_function = pytest_benchmark.plugin.benchmark
        try:
            # pytest >= 8.4.0
            wrapped_func = fixture_function._get_wrapped_function()
        except AttributeError:
            wrapped_func = fixture_function.__pytest_wrapped__.obj
        assert inspect.isgeneratorfunction(wrapped_func)

        generator = wrapped_func(request)
        bench = next(generator)
        assert isinstance(bench, pytest_benchmark.plugin.BenchmarkFixture)
        request.addfinalizer(lambda: next(generator, None))

        suffix = f"-{name}"

        def add_suffix(_name):
            start, sep, end = _name.partition("[")
            return start + suffix + sep + end

        bench.name = add_suffix(bench.name)
        bench.fullname = add_suffix(bench.fullname)

        return bench

    return _make_bench


@pytest.fixture
def bench_dvc(request, dvc_bin, make_bench):
    def _bench_dvc(*args, **kwargs):
        name = kwargs.pop("name", None)
        name = f"-{name}" if name else ""
        bench = make_bench(args[0] + name)
        if request.config.getoption("--dvc-benchmark-cprofile-dump") or kwargs.pop(
            "cprofile", False
        ):
            cprofile_results = request.config.invocation_params.dir / "prof"
            cprofile_results.mkdir(exist_ok=True)
            stats_file = cprofile_results / f"{bench.name}.prof"
            args = (*args, "--cprofile-dump", stats_file)

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
def make_dataset(request, bench_config, tmp_dir, dvc_bench_repo):
    def _make_dataset(
        dvcfile=False, files=True, cache=False, commit=False, remote=False
    ):
        from dvc.repo import Repo

        path = tmp_dir / "dataset"
        root = dvc_bench_repo
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
    url = bench_config.project_repo

    if os.path.isdir(url):
        path = url
        assert not rev
    else:
        path = make_project(url, rev=rev)

    monkeypatch.chdir(path)

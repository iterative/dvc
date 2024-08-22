import os
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Optional

DEFAULT_DVC_BIN = "dvc"
DEFAULT_DVC_REPO = os.fspath(Path(__file__).parents[3])
DEFAULT_PROJECT_REPO = "https://github.com/iterative/example-get-started"


def pytest_report_header(config):
    bconf = config.bench_config
    return f"dvc-bench: {bconf}"


def pytest_generate_tests(metafunc):
    revs = metafunc.config.getoption("--dvc-revs")
    if not revs:
        revs = [None]
    if "dvc_rev" in metafunc.fixturenames:
        metafunc.parametrize("dvc_rev", revs, scope="session")


@dataclass
class DVCBenchConfig:
    dataset: str = "tiny"
    dvc_repo: str = DEFAULT_DVC_REPO
    dvc_bench_repo: Optional[str] = None
    project_repo: str = DEFAULT_PROJECT_REPO
    project_rev: Optional[str] = None
    dvc_bin: str = DEFAULT_DVC_BIN
    dvc_revs: Optional[list[str]] = None
    dvc_install_deps: Optional[str] = None

    def __repr__(self):
        args = ", ".join(
            f"{f.name}={val!r}"
            for f in fields(self)
            if (val := getattr(self, f.name)) != f.default
        )
        return f"{self.__class__.__name__}({args})"


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "requires(spec): mark a test to run only on versions that satisfy the spec",
    )
    config.bench_config = DVCBenchConfig(
        dataset=config.getoption("--dataset"),
        dvc_repo=config.getoption("--dvc-repo"),
        dvc_bench_repo=config.getoption("--dvc-bench-repo"),
        project_repo=config.getoption("--project-repo"),
        project_rev=config.getoption("--project-rev"),
        dvc_bin=config.getoption("--dvc-bin"),
        dvc_revs=config.getoption("--dvc-revs"),
        dvc_install_deps=config.getoption("--dvc-install-deps"),
    )


def resolve_path(path):
    if os.path.isdir(path):
        return os.path.abspath(path)
    return path


def pytest_addoption(parser):
    parser.addoption(
        "--dataset",
        type=str,
        default="tiny",
        help="Dataset name to use in tests (e.g. tiny/small/large/mnist/etc)",
    )

    parser.addoption(
        "--benchmark-cprofile-dump",
        action="store_true",
        default=False,
        help="Save cprofile results",
    )

    parser.addoption(
        "--dvc-bin",
        type=str,
        default=DEFAULT_DVC_BIN,
        help="Path to dvc binary",
    )

    parser.addoption(
        "--dvc-revs",
        type=lambda revs: revs.split(","),
        help=("Comma-separated list of DVC revisions to test (overrides `--dvc-bin`)"),
    )

    parser.addoption(
        "--dvc-repo",
        type=resolve_path,
        default=DEFAULT_DVC_REPO,
        help="Path or url to dvc git repo",
    )

    parser.addoption(
        "--dvc-install-deps",
        type=str,
        help="Comma-separated list of DVC installation packages",
    )

    parser.addoption(
        "--dvc-bench-repo",
        type=resolve_path,
        default=None,
        help="Path or url to dvc-bench git repo (for loading benchmark datasets)",
    )

    parser.addoption("--project-rev", type=str, help="Project revision to test")
    parser.addoption(
        "--project-repo",
        type=resolve_path,
        default=DEFAULT_PROJECT_REPO,
        help="Path or url to dvc project",
    )

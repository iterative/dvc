DEFAULT_DVC_BIN = "dvc"
DEFAULT_DVC_GIT_REPO = "https://github.com/iterative/dvc"
DEFAULT_DVC_BENCH_GIT_REPO = "https://github.com/iterative/dvc-bench"
DEFAULT_PROJECT_GIT_REPO = "https://github.com/iterative/example-get-started"


def pytest_report_header(config):
    bconf = config.bench_config
    return "dvc-bench: (" f"dataset: '{bconf.dataset}'," f"revs: '{bconf.dvc_revs}'" ")"


def pytest_generate_tests(metafunc):
    str_revs = metafunc.config.getoption("--dvc-revs")
    revs = str_revs.split(",") if str_revs else [None]
    if "dvc_rev" in metafunc.fixturenames:
        metafunc.parametrize("dvc_rev", revs, scope="session")


class DVCBenchConfig:
    def __init__(self):
        self.dataset = "small"
        self.dvc_bin = DEFAULT_DVC_BIN
        self.dvc_revs = None
        self.dvc_git_repo = DEFAULT_DVC_GIT_REPO
        self.dvc_install_deps = None
        self.dvc_bench_git_repo = DEFAULT_DVC_BENCH_GIT_REPO
        self.project_rev = None
        self.project_git_repo = DEFAULT_PROJECT_GIT_REPO


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "requires(minversion): mark a test as requiring minimum DVC version"
    )

    config.bench_config = DVCBenchConfig()
    config.bench_config.dataset = config.getoption("--dataset")
    config.bench_config.dvc_bin = config.getoption("--dvc-bin")
    config.bench_config.dvc_revs = config.getoption("--dvc-revs")
    config.bench_config.dvc_git_repo = config.getoption("--dvc-git-repo")
    config.bench_config.dvc_install_deps = config.getoption("--dvc-install-deps")
    config.bench_config.dvc_bench_git_repo = config.getoption("--dvc-bench-git-repo")
    config.bench_config.project_rev = config.getoption("--project-rev")
    config.bench_config.project_git_repo = config.getoption("--project-git-repo")


def pytest_addoption(parser):
    parser.addoption(
        "--dataset",
        type=str,
        default="small",
        help="Dataset name to use in tests (e.g. tiny/small/large/mnist/etc)",
    )

    parser.addoption(
        "--dvc-bin",
        type=str,
        default=DEFAULT_DVC_BIN,
        help="Path to dvc binary",
    )

    parser.addoption(
        "--dvc-revs",
        type=str,
        help=("Comma-separated list of DVC revisions to test (overrides `--dvc-bin`)"),
    )

    parser.addoption(
        "--dvc-git-repo",
        type=str,
        default=DEFAULT_DVC_GIT_REPO,
        help="Path or url to dvc git repo",
    )

    parser.addoption(
        "--dvc-install-deps",
        type=str,
        default="",
        help="Comma-separated list of DVC installation packages",
    )

    parser.addoption(
        "--dvc-bench-git-repo",
        type=str,
        default=DEFAULT_DVC_BENCH_GIT_REPO,
        help="Path or url to dvc-bench git repo (for loading benchmark datasets)",
    )

    parser.addoption(
        "--project-rev",
        type=str,
        help="Project revision to test",
    )

    parser.addoption(
        "--project-git-repo",
        type=str,
        default=DEFAULT_PROJECT_GIT_REPO,
        help="Path or url to dvc project",
    )

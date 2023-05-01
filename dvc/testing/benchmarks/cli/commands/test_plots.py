import pytest

from dvc.repo import Repo
from tests.benchmarks.conftest import pull

# pylint: disable=unused-argument


@pytest.mark.requires(minversion=(2, 34, 0), reason="top-level plots not supported")
def test_plots(project, bench_dvc):
    with Repo() as dvc:
        pull(dvc)

    kwargs = {"rounds": 5, "iterations": 3, "warmup_rounds": 2}
    bench_dvc("plots", "show", name="show", **kwargs)
    bench_dvc("plots", "show", "--json", name="show-json", **kwargs)
    bench_dvc("plots", "diff", "HEAD", name="diff", **kwargs)
    bench_dvc("plots", "diff", "HEAD", "--json", name="diff-json", **kwargs)

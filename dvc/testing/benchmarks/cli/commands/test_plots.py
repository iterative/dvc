import pytest

from dvc.repo import Repo
from dvc.testing.benchmarks.fixtures import _pull


@pytest.mark.requires(minversion=(2, 34, 0), reason="top-level plots not supported")
def test_plots(project, bench_dvc):
    with Repo() as dvc:
        _pull(dvc)

    kwargs = {"rounds": 5, "iterations": 3, "warmup_rounds": 2}
    bench_dvc("plots", "show", name="show", **kwargs)
    bench_dvc("plots", "show", "--json", name="show-json", **kwargs)
    bench_dvc("plots", "diff", "HEAD", name="diff", **kwargs)
    bench_dvc("plots", "diff", "HEAD", "--json", name="diff-json", **kwargs)

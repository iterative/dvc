import pytest


@pytest.mark.flaky(reruns=3)
@pytest.mark.requires(
    "!=3.53.*,!=3.54.0",
    reason="Takes 10 mins to run. Regression in 3.53.0, fixed in 3.54.1",
)
# Introduced in https://github.com/iterative/dvc/pull/10388.
# Fixed in https://github.com/iterative/dvc/pull/10531.
def test_import(bench_dvc, tmp_dir, scm, dvc, make_dataset, remote):
    dataset = make_dataset(
        cache=False, files=False, dvcfile=True, commit=True, remote=True
    )
    bench_dvc("import", tmp_dir, dataset.name, "-o", "new")

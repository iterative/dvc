import pytest


@pytest.mark.flaky(reruns=3)
def test_get(bench_dvc, tmp_dir, scm, dvc, make_dataset, remote):
    dataset = make_dataset(
        cache=False, files=False, dvcfile=True, commit=True, remote=True
    )
    bench_dvc("get", f"file://{tmp_dir.as_posix()}", dataset.name, "-o", "new")

# pylint: disable=unused-argument
def test_get(bench_dvc, tmp_dir, scm, dvc, make_dataset, remote):
    dataset = make_dataset(
        cache=False, files=False, dvcfile=True, commit=True, remote=True
    )
    bench_dvc("get", tmp_dir, dataset.name, "-o", "new")

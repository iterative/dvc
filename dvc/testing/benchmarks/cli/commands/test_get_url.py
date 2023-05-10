# pylint: disable=unused-argument
def test_get_url(bench_dvc, tmp_dir, scm, dvc, make_dataset):
    dataset = make_dataset(
        cache=False, files=True, dvcfile=False, commit=False, remote=False
    )
    bench_dvc("get-url", str(dataset), "new")

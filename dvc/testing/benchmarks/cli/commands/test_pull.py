# pylint: disable=unused-argument
def test_pull(bench_dvc, tmp_dir, dvc, make_dataset, remote):
    make_dataset(cache=False, dvcfile=True, files=False, remote=True)
    bench_dvc("pull")

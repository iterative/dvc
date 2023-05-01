# pylint: disable=unused-argument
def test_push(bench_dvc, tmp_dir, dvc, make_dataset, remote):
    make_dataset(cache=True, dvcfile=True, files=False)
    bench_dvc("push")

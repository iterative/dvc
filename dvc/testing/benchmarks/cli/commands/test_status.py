# pylint: disable=unused-argument
def test_status(bench_dvc, tmp_dir, dvc, make_dataset):
    dataset = make_dataset(files=True, dvcfile=True, cache=True)
    bench_dvc("status")
    bench_dvc("status", name="noop")

    (dataset / "new").write_text("new")
    bench_dvc("status", name="changed")
    bench_dvc("status", name="changed-noop")

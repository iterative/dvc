# pylint: disable=unused-argument
def test_diff(bench_dvc, tmp_dir, scm, dvc, make_dataset):
    dataset = make_dataset(cache=True, files=True, dvcfile=True, commit=True)
    bench_dvc("diff")
    bench_dvc("diff", name="noop")

    (dataset / "new").write_text("new")
    bench_dvc("diff", name="changed")
    bench_dvc("diff", name="changed-noop")

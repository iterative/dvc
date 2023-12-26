def test_update(bench_dvc, tmp_dir, dvc, make_dataset):
    dataset = make_dataset(
        cache=False, files=True, dvcfile=False, commit=False, remote=False
    )
    bench_dvc("import-url", str(dataset), "new")
    (dataset / "new").write_text("new")
    bench_dvc("update", "new.dvc")
    bench_dvc("update", "new.dvc", name="noop")

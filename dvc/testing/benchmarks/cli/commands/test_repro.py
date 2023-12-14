def test_repro(bench_dvc, tmp_dir, dvc, make_dataset, run_copy):
    dataset = make_dataset(dvcfile=True, files=True, cache=True)
    source_path = dataset.relative_to(tmp_dir).as_posix()
    run_copy(source_path, "copy-1", name="copy-1")
    run_copy(source_path, "copy-2", name="copy-2")
    run_copy("copy-1", "copy-3", name="copy-3")
    bench_dvc("repro")
    bench_dvc("repro", name="noop")

def test_add(bench_dvc, tmp_dir, dvc, dataset):
    bench_dvc("add", dataset)

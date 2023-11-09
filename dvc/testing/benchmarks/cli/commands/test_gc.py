def test_gc(bench_dvc, tmp_dir, dvc, make_dataset):
    make_dataset(files=False, cache=True, dvcfile=False)
    bench_dvc("gc", "-f", "-w")

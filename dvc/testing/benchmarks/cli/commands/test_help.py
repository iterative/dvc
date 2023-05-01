def test_help(bench_dvc):
    bench_dvc("--help", rounds=100)

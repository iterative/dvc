import shutil


def test_sharing(bench_dvc, tmp_dir, dvc, make_dataset, remote):
    dataset = make_dataset(cache=True, dvcfile=True)

    bench_dvc("push")
    bench_dvc("push", name="noop")

    shutil.rmtree(dataset)
    shutil.rmtree(tmp_dir / ".dvc" / "cache")

    bench_dvc("fetch")
    bench_dvc("fetch", name="noop")

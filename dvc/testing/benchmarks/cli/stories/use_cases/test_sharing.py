import shutil

# pylint: disable=unused-argument


def test_sharing(bench_dvc, tmp_dir, dvc, dataset, remote):
    bench_dvc("add", dataset)
    bench_dvc("add", dataset, name="noop")

    bench_dvc("push")
    bench_dvc("push", name="noop")

    shutil.rmtree(dataset)
    shutil.rmtree(tmp_dir / ".dvc" / "cache")

    bench_dvc("pull")
    bench_dvc("pull", name="noop")

    bench_dvc("checkout", name="noop")

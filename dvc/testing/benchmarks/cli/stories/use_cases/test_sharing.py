import shutil

from packaging import version


def test_sharing(bench_dvc, tmp_dir, dvc, make_dataset, remote, dvc_bin):
    is_gs = dvc.config["remote"]["upstream"]["url"].startswith("gs://")
    if is_gs and version.Version(dvc_bin.version) < version.Version("3.59.2"):
        # support for allow_anonymous_login in gs was introduced in dvc==3.60
        # This should not impact the test, just that it will make it slower if used
        # with fake-gcs-server.
        with dvc.config.edit() as d:
            d["remote"]["upstream"].pop("allow_anonymous_login", None)

    dataset = make_dataset(cache=True, dvcfile=True)

    bench_dvc("push")
    bench_dvc("push", name="noop")

    shutil.rmtree(dataset)
    shutil.rmtree(tmp_dir / ".dvc" / "cache")

    bench_dvc("fetch")
    bench_dvc("fetch", name="noop")

from .test_checkout import _skip_unsupported_link


def generate_test(*, link_type="copy"):
    def _test_add(bench_dvc, tmp_dir, dvc, dataset):
        _skip_unsupported_link((tmp_dir / ".dvc" / "cache"), tmp_dir, link_type)

        with dvc.config.edit() as conf:
            conf["cache"]["type"] = link_type

        bench_dvc("add", dataset)
        bench_dvc("add", dataset, name="noop")

    return _test_add


test_add_copy = generate_test(link_type="copy")
test_add_symlink = generate_test(link_type="symlink")
test_add_hardlink = generate_test(link_type="hardlink")

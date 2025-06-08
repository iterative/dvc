import os

import pytest

from dvc.fs import localfs
from dvc_objects.fs import generic
from dvc_objects.fs.utils import tmp_fname


def _skip_unsupported_link(src, dest, link_type):
    src_test_file = os.path.join(src, tmp_fname())
    dest_test_file = os.path.join(dest, tmp_fname())
    if not generic.test_links(
        [link_type], localfs, src_test_file, localfs, dest_test_file
    ):
        pytest.skip(f"{link_type} not supported")


def generate_test(*, link_type="copy"):
    def _test_checkout_func(bench_dvc, tmp_dir, dvc, make_dataset):
        dataset = make_dataset(dvcfile=True, cache=True, files=False)

        _skip_unsupported_link((tmp_dir / ".dvc" / "cache"), tmp_dir, link_type)

        with dvc.config.edit() as conf:
            conf["cache"]["type"] = link_type

        bench_dvc("checkout", dataset)
        bench_dvc("checkout", name="noop")
        (dataset / "new").write_text("new")
        bench_dvc("checkout", "--force", name="update")

    return _test_checkout_func


test_checkout_copy = generate_test(link_type="copy")
test_checkout_symlink = generate_test(link_type="symlink")
test_checkout_hardlink = generate_test(link_type="hardlink")

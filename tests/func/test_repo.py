import os

from dulwich.porcelain import push as git_push
from dulwich.porcelain import remote_add as git_remote_add

from dvc.cachemgr import CacheManager
from dvc.dvcfile import LOCK_FILE, PROJECT_FILE
from dvc.fs import system
from dvc.repo import Repo
from dvc.scm import Git


def test_open_bare(tmp_dir, scm, dvc, tmp_path_factory):
    tmp_dir.dvc_gen(
        {
            "dir123": {"foo": "foo content"},
            "dirextra": {"extrafoo": "extra foo content"},
        },
        commit="initial",
    )

    url = os.fspath(tmp_path_factory.mktemp("bare"))
    Git.init(url, bare=True).close()

    git_remote_add(tmp_dir, "origin", url)
    git_push(tmp_dir, "origin")

    with Repo.open(url) as repo:
        assert repo.scm.root_dir != url

    with Repo.open(url, uninitialized=True) as repo:
        assert repo.scm.root_dir != url


def test_destroy(tmp_dir, dvc, run_copy):
    dvc.config["cache"]["type"] = ["symlink"]
    dvc.cache = CacheManager(dvc)

    tmp_dir.dvc_gen("file", "text")
    tmp_dir.dvc_gen({"dir": {"file": "lorem", "subdir/file": "ipsum"}})

    run_copy("file", "file2", name="copy-file-file2")
    run_copy("file2", "file3", name="copy-file2-file3")
    run_copy("file3", "file4", name="copy-file3-file4")

    dvc.destroy()

    # Remove all the files related to DVC
    for path in [
        ".dvc",
        ".dvcignore",
        "file.dvc",
        "dir.dvc",
        PROJECT_FILE,
        LOCK_FILE,
    ]:
        assert not (tmp_dir / path).exists()

    # Leave the rest of the files
    for path in [
        "file",
        "file2",
        "file3",
        "file4",
        "dir/file",
        "dir/subdir/file",
    ]:
        assert (tmp_dir / path).is_file()

    # Make sure that data was unprotected after `destroy`
    for path in [
        "file",
        "file2",
        "file3",
        "file4",
        "dir",
        "dir/file",
        "dir/subdir",
        "dir/subdir/file",
    ]:
        assert not system.is_symlink(tmp_dir / path)

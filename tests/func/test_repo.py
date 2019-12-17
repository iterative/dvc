import os

from dvc.scm.git.tree import GitTree
from dvc.cache import Cache
from dvc.repo import Repo
from dvc.system import System
from dvc.utils.compat import fspath


def test_destroy(tmp_dir, dvc):
    dvc.config.set("cache", "type", "symlink")
    dvc.cache = Cache(dvc)

    tmp_dir.dvc_gen("file", "text")
    tmp_dir.dvc_gen({"dir": {"file": "lorem", "subdir/file": "ipsum"}})

    dvc.destroy()

    # Remove all the files related to DVC
    for path in [".dvc", "file.dvc", "dir.dvc"]:
        assert not (tmp_dir / path).exists()

    # Leave the rest of the files
    for path in ["file", "dir/file", "dir/subdir/file"]:
        assert (tmp_dir / path).is_file()

    # Make sure that data was unprotected after `destroy`
    for path in ["file", "dir", "dir/file", "dir/subdir", "dir/subdir/file"]:
        assert not System.is_symlink(fspath(tmp_dir / path))


def test_collect(tmp_dir, scm, dvc, run_copy):
    def collect_outs(*args, **kwargs):
        return set(
            str(out.path_info)
            for stage in dvc.collect(*args, **kwargs)
            for out in stage.outs
        )

    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar")
    scm.add([".gitignore", "foo.dvc", "bar.dvc"])
    scm.commit("Add foo and bar")

    scm.checkout("new-branch", create_new=True)

    run_copy("bar", "buzz")
    scm.add([".gitignore", "buzz.dvc"])
    scm.commit("Add buzz")

    assert collect_outs("bar.dvc", with_deps=True) == {"foo", "bar"}

    dvc.tree = GitTree(scm.repo, "new-branch")

    assert collect_outs("buzz.dvc", with_deps=True) == {"foo", "bar", "buzz"}
    assert collect_outs("buzz.dvc", with_deps=False) == {"buzz"}


def test_stages(tmp_dir, dvc):
    def stages():
        return set(stage.relpath for stage in Repo(fspath(tmp_dir)).stages)

    tmp_dir.dvc_gen({"file": "a", "dir/file": "b", "dir/subdir/file": "c"})

    assert stages() == {
        "file.dvc",
        os.path.join("dir", "file.dvc"),
        os.path.join("dir", "subdir", "file.dvc"),
    }

    tmp_dir.gen(".dvcignore", "dir")

    assert stages() == {"file.dvc"}

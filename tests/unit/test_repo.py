from dvc.repo import Repo
from dvc.scm.git import GitTree
from dvc.system import System
from dvc.utils.compat import fspath


def test_is_dvc_internal(tmp_dir, dvc):
    assert dvc.is_dvc_internal(fspath(tmp_dir / ".dvc" / "file"))
    assert not dvc.is_dvc_internal("file")


def test_destroy(tmp_dir, dvc):
    dvc.config.set("cache", "type", "symlink")

    tmp_dir.dvc_gen('file', 'text')
    tmp_dir.dvc_gen({'dir': {'file': 'lorem', 'subdir/file': 'ipsum'}})

    dvc.destroy()

    # Remove all the files related to DVC
    assert not (tmp_dir / ".dvc").exists()
    assert not (tmp_dir / "file.dvc").exists()
    assert not (tmp_dir / "dir.dvc").exists()

    # Leave the rest of the files
    assert (tmp_dir / "file").is_file()
    assert (tmp_dir / "dir").is_dir()
    assert (tmp_dir / "dir" / "file").is_file()
    assert (tmp_dir / "dir" / "subdir" / "file").is_file()

    # Make sure that data was unprotected after `destroy`
    assert not System.is_symlink(fspath(tmp_dir / "foo"))
    assert not System.is_symlink(fspath(tmp_dir / "file"))
    assert not System.is_symlink(fspath(tmp_dir / "dir"))
    assert not System.is_symlink(fspath(tmp_dir / "dir" / "file"))
    assert not System.is_symlink(fspath(tmp_dir / "dir" / "subdir" / "file"))


def test_collect(tmp_dir, scm, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar")
    scm.add([".gitignore", "foo.dvc", "bar.dvc"])
    scm.commit("Add foo and bar")

    scm.checkout("new-branch", create_new=True)

    run_copy("bar", "buzz")
    scm.add([".gitignore", "buzz.dvc"])
    scm.commit("Add buzz")

    def collect_outs(*args, **kwargs):
        return [
            str(out.path_info)
            for stage in dvc.collect(*args, **kwargs)
            for out in stage.outs
        ]

    assert ["foo", "bar"] == collect_outs("bar.dvc", with_deps=True)

    dvc.tree = GitTree(scm.repo, "new-branch")

    assert ["foo", "bar", "buzz"] == collect_outs("buzz.dvc", with_deps=True)
    assert ["buzz"] == collect_outs("buzz.dvc", with_deps=False)


def test_stages(tmp_dir, dvc):
    tmp_dir.dvc_gen({"file": "a", "dir/file": "b", "dir/subdir/file": "c"})

    def stages():
        return [stage.relpath for stage in Repo(tmp_dir).stages]

    assert stages() == ["file.dvc", "dir/file.dvc", "dir/subdir/file.dvc"]

    tmp_dir.gen(".dvcignore", "dir")

    assert stages() == ["file.dvc"]

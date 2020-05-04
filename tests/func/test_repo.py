import os

from dvc.cache import Cache
from dvc.repo import Repo
from dvc.system import System
from dvc.compat import fspath


def test_destroy(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import PIPELINE_FILE, PIPELINE_LOCK

    dvc.config["cache"]["type"] = ["symlink"]
    dvc.cache = Cache(dvc)

    tmp_dir.dvc_gen("file", "text")
    tmp_dir.dvc_gen({"dir": {"file": "lorem", "subdir/file": "ipsum"}})

    run_copy("file", "file2", single_stage=True)
    run_copy("file2", "file3", name="copy-file2-file3")
    run_copy("file3", "file4", name="copy-file3-file4")

    dvc.destroy()

    # Remove all the files related to DVC
    for path in [
        ".dvc",
        "file.dvc",
        "file2.dvc",
        "dir.dvc",
        PIPELINE_FILE,
        PIPELINE_LOCK,
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
        assert not System.is_symlink(fspath(tmp_dir / path))


def test_collect(tmp_dir, scm, dvc, run_copy):
    def collect_outs(*args, **kwargs):
        return set(
            str(out)
            for stage in dvc.collect(*args, **kwargs)
            for out in stage.outs
        )

    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", single_stage=True)
    scm.add([".gitignore", "foo.dvc", "bar.dvc"])
    scm.commit("Add foo and bar")

    scm.checkout("new-branch", create_new=True)

    run_copy("bar", "buzz", single_stage=True)
    scm.add([".gitignore", "buzz.dvc"])
    scm.commit("Add buzz")

    assert collect_outs("bar.dvc", with_deps=True) == {"foo", "bar"}
    assert collect_outs("buzz.dvc", with_deps=True) == {"foo", "bar", "buzz"}
    assert collect_outs("buzz.dvc", with_deps=False) == {"buzz"}

    run_copy("foo", "foobar", name="copy-foo-foobar")
    assert collect_outs(":copy-foo-foobar") == {"foobar"}
    assert collect_outs(":copy-foo-foobar", with_deps=True) == {
        "foobar",
        "foo",
    }
    assert collect_outs("dvc.yaml:copy-foo-foobar", recursive=True) == {
        "foobar"
    }

    run_copy("foobar", "baz", name="copy-foobar-baz")
    assert collect_outs("dvc.yaml") == {"foobar", "baz"}
    assert collect_outs("dvc.yaml", with_deps=True) == {
        "foobar",
        "baz",
        "foo",
    }


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

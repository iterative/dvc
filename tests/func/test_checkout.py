import logging
import os
import shutil
import stat
import textwrap

import pytest

from dvc.cli import main
from dvc.dvcfile import PIPELINE_FILE, Dvcfile
from dvc.exceptions import (
    CheckoutError,
    CheckoutErrorSuggestGit,
    ConfirmRemoveError,
    DvcException,
    NoOutputOrStageError,
)
from dvc.fs import LocalFileSystem, system
from dvc.stage import Stage
from dvc.stage.exceptions import StageFileDoesNotExistError
from dvc.utils import relpath
from dvc.utils.fs import remove
from dvc.utils.serialize import dump_yaml, load_yaml
from tests.utils import get_gitignore_content

logger = logging.getLogger("dvc")


def walk_files(directory):
    for root, _, files in os.walk(directory):
        for f in files:
            yield os.path.join(root, f)


def test_checkout(tmp_dir, dvc, copy_script):
    tmp_dir.dvc_gen({"foo": "foo", "data": {"file": "file"}})
    dvc.run(
        fname="file1.dvc",
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        single_stage=True,
    )
    remove(tmp_dir / "foo")
    remove("data")

    dvc.checkout(force=True)
    assert (tmp_dir / "foo").read_text() == "foo"
    assert (tmp_dir / "data").read_text() == {"file": "file"}


def test_checkout_cli(tmp_dir, dvc, copy_script):
    tmp_dir.dvc_gen({"foo": "foo", "data": {"file": "file"}})
    dvc.run(
        fname="file1.dvc",
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        single_stage=True,
    )
    remove(tmp_dir / "foo")
    remove("data")

    assert main(["checkout", "--force"]) == 0
    assert (tmp_dir / "foo").read_text() == "foo"
    assert (tmp_dir / "data").read_text() == {"file": "file"}

    remove(tmp_dir / "foo")
    remove("data")

    assert main(["checkout", "--force", "foo.dvc"]) == 0
    assert main(["checkout", "--force", "data.dvc"]) == 0
    assert (tmp_dir / "foo").read_text() == "foo"
    assert (tmp_dir / "data").read_text() == {"file": "file"}


def test_checkout_corrupted_cache_file(tmp_dir, dvc):
    (foo_stage,) = tmp_dir.dvc_gen("foo", "foo")
    cache = foo_stage.outs[0].cache_path

    os.chmod(cache, 0o644)
    with open(cache, "a", encoding="utf-8") as fd:
        fd.write("1")

    with pytest.raises(CheckoutError):
        dvc.checkout(force=True)

    assert not os.path.isfile("foo")
    assert not os.path.isfile(cache)


def test_checkout_corrupted_cache_dir(tmp_dir, dvc):
    from dvc_data.hashfile import load

    tmp_dir.gen("data", {"foo": "foo", "bar": "bar"})
    # NOTE: using 'copy' so that cache and link don't have same inode
    ret = main(["config", "cache.type", "copy"])
    assert ret == 0

    dvc.config.load()

    stages = dvc.add("data")
    assert len(stages) == 1
    assert len(stages[0].outs) == 1
    out = stages[0].outs[0]

    # NOTE: modifying cache file for one of the files inside the directory
    # to check if dvc will detect that the cache is corrupted.
    obj = load(dvc.odb.local, out.hash_info)
    _, _, entry_oid = list(obj)[0]
    cache = dvc.odb.local.oid_to_path(entry_oid.value)

    os.chmod(cache, 0o644)
    with open(cache, "w+", encoding="utf-8") as fobj:
        fobj.write("1")

    with pytest.raises(CheckoutError):
        dvc.checkout(force=True)

    assert not os.path.exists(cache)


def test_remove_files_when_checkout(tmp_dir, dvc, scm):
    # add the file into a separate branch
    scm.checkout("branch", True)
    ret = main(["checkout", "--force"])
    assert ret == 0
    tmp_dir.dvc_gen("file_in_a_branch", "random text", commit="add file")

    # Checkout back in master
    scm.checkout("master")
    assert os.path.exists("file_in_a_branch")

    # Make sure `dvc checkout` removes the file
    # self.dvc.checkout()
    ret = main(["checkout", "--force"])
    assert ret == 0
    assert not os.path.exists("file_in_a_branch")


class TestCheckoutCleanWorkingDir:
    def test(self, mocker, tmp_dir, dvc):  # pylint: disable=unused-argument
        mock_prompt = mocker.patch("dvc.prompt.confirm", return_value=True)
        (stage,) = tmp_dir.dvc_gen("data", {"foo": "foo"})

        # change working directory
        (tmp_dir / "data").gen("not_cached.txt", "not_cached")
        assert main(["checkout", stage.relpath]) == 0
        mock_prompt.assert_called()
        assert not (tmp_dir / "data" / "not_cached.txt").exists()

    def test_force(self, mocker, tmp_dir, dvc):
        mock_prompt = mocker.patch("dvc.prompt.confirm", return_value=False)
        (stage,) = tmp_dir.dvc_gen("data", {"foo": "foo"})

        # change working directory
        (tmp_dir / "data").gen("not_cached.txt", "not_cached")
        assert main(["checkout", stage.relpath]) != 0
        mock_prompt.assert_called()
        assert (tmp_dir / "data" / "not_cached.txt").exists()


def test_checkout_selective_remove(tmp_dir, dvc):
    # Use copy to test for changes in the inodes
    dvc.odb.local.cache_types = ["copy"]
    tmp_dir.dvc_gen({"data": {"foo": "foo", "bar": "bar"}})

    foo_inode = system.inode(os.path.join("data", "foo"))
    bar_inode = system.inode(os.path.join("data", "bar"))
    # move instead of remove, to lock inode assigned to stage_files[0].path
    # if we were to use remove, we might end up with same inode assigned to
    # newly checked out file
    shutil.move(os.path.join("data", "foo"), "random_name")

    assert main(["checkout", "--force", "data.dvc"]) == 0
    assert (tmp_dir / "data").read_text() == {"foo": "foo", "bar": "bar"}
    assert system.inode(os.path.join("data", "foo")) != foo_inode
    assert system.inode(os.path.join("data", "bar")) == bar_inode


def test_gitignore_basic(tmp_dir, dvc, scm):
    tmp_dir.gen("foo", "foo")
    assert not os.path.exists(scm.GITIGNORE)

    tmp_dir.dvc_gen("file1", "random text1", commit="add file1")
    tmp_dir.dvc_gen("file2", "random text2", commit="add file2")
    dvc.run(
        single_stage=True,
        cmd="cp foo file3",
        deps=["foo"],
        outs_no_cache=["file3"],
    )
    assert get_gitignore_content() == ["/file1", "/file2"]


def test_gitignore_when_checkout(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen("file_in_a_master", "master", commit="master")

    scm.checkout("branch", True)
    ret = main(["checkout", "--force"])
    assert ret == 0
    tmp_dir.dvc_gen("file_in_a_branch", "branch", commit="branch")

    scm.checkout("master")
    ret = main(["checkout", "--force"])
    assert ret == 0

    ignored = get_gitignore_content()

    assert len(ignored) == 1
    assert "/file_in_a_master" in ignored

    scm.checkout("branch")
    ret = main(["checkout", "--force"])
    assert ret == 0
    ignored = get_gitignore_content()
    assert "/file_in_a_branch" in ignored


def test_checkout_missing_md5_in_stage_file(tmp_dir, dvc, copy_script):
    tmp_dir.dvc_gen({"foo": "foo", "data": {"file": "file"}})
    stage = dvc.run(
        fname="file1.dvc",
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        single_stage=True,
    )
    d = load_yaml(stage.relpath)
    del d[Stage.PARAM_OUTS][0][LocalFileSystem.PARAM_CHECKSUM]
    del d[Stage.PARAM_DEPS][0][LocalFileSystem.PARAM_CHECKSUM]
    dump_yaml(stage.relpath, d)

    with pytest.raises(CheckoutError):
        dvc.checkout(force=True)


def test_checkout_empty_dir(tmp_dir, dvc):
    empty_dir = tmp_dir / "empty_dir"
    empty_dir.mkdir()
    (stage,) = dvc.add("empty_dir")

    stage.outs[0].remove()
    assert not empty_dir.exists()

    stats = dvc.checkout(force=True)
    assert stats["added"] == [os.path.join("empty_dir", "")]
    assert empty_dir.is_dir()
    assert not list(empty_dir.iterdir())


def test_checkout_not_cached_file(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    dvc.run(
        cmd="cp foo bar",
        deps=["foo"],
        outs_no_cache=["bar"],
        single_stage=True,
    )
    stats = dvc.checkout(force=True)
    assert not any(stats.values())


def test_checkout_with_deps_cli(tmp_dir, dvc, copy_script):
    tmp_dir.dvc_gen({"foo": "foo", "data": {"file": "file"}})
    dvc.run(
        fname="file1.dvc",
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        single_stage=True,
    )
    remove("foo")
    remove("file1")

    assert not os.path.exists("foo")
    assert not os.path.exists("file1")

    ret = main(["checkout", "--force", "file1.dvc", "--with-deps"])
    assert ret == 0

    assert os.path.exists("foo")
    assert os.path.exists("file1")


def test_checkout_directory(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen({"data": {"foo": "foo", "bar": "bar"}})

    remove("data")
    assert not os.path.exists("data")

    ret = main(["checkout", stage.path])
    assert ret == 0

    assert os.path.exists("data")


def test_checkout_hook(mocker, tmp_dir, dvc):
    """Test that dvc checkout handles EOFError gracefully, which is what
    it will experience when running in a git hook.
    """
    tmp_dir.dvc_gen({"data": {"foo": "foo"}})
    mocker.patch("sys.stdout.isatty", return_value=True)
    mocker.patch("dvc.prompt.input", side_effect=EOFError)

    (tmp_dir / "data").gen("test", "test")
    with pytest.raises(ConfirmRemoveError):
        dvc.checkout()


def test_checkout_suggest_git(tmp_dir, dvc, scm):
    # pylint: disable=no-member
    tmp_dir.dvc_gen("foo", "foo")
    try:
        dvc.checkout(targets="gitbranch")
    except DvcException as exc:
        assert isinstance(exc, CheckoutErrorSuggestGit)
        assert isinstance(exc.__cause__, NoOutputOrStageError)
        assert isinstance(exc.__cause__.__cause__, StageFileDoesNotExistError)

    try:
        dvc.checkout(targets="foo")
    except DvcException as exc:
        assert isinstance(exc, CheckoutErrorSuggestGit)
        assert isinstance(exc.__cause__, NoOutputOrStageError)
        assert exc.__cause__.__cause__ is None

    try:
        dvc.checkout(targets="looks-like-dvcfile.dvc")
    except DvcException as exc:
        assert isinstance(exc, CheckoutErrorSuggestGit)
        assert isinstance(exc.__cause__, StageFileDoesNotExistError)
        assert exc.__cause__.__cause__ is None


def test_checkout_target_recursive_should_not_remove_other_used_files(
    tmp_dir, dvc
):
    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar", "data": {"file": "file"}})
    assert main(["checkout", "-R", "data"]) == 0
    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / "bar").exists()


def test_checkout_recursive_not_directory(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    ret = main(["add", "foo"])
    assert ret == 0

    stats = dvc.checkout(targets=["foo" + ".dvc"], recursive=True)
    assert stats == {"added": [], "modified": [], "deleted": []}


def test_checkout_moved_cache_dir_with_symlinks(tmp_dir, dvc):
    tmp_dir.gen({"foo": "foo", "data": {"file": "file"}})
    ret = main(["config", "cache.type", "symlink"])
    assert ret == 0

    ret = main(["add", "foo"])
    assert ret == 0

    ret = main(["add", "data"])
    assert ret == 0

    assert system.is_symlink("foo")
    old_foo_link = os.path.realpath("foo")

    assert system.is_symlink(os.path.join("data", "file"))
    old_data_link = os.path.realpath(os.path.join("data", "file"))

    old_cache_dir = dvc.odb.local.path
    new_cache_dir = old_cache_dir + "_new"
    os.rename(old_cache_dir, new_cache_dir)

    ret = main(["cache", "dir", new_cache_dir])
    assert ret == 0

    ret = main(["checkout", "-f"])
    assert ret == 0

    assert system.is_symlink("foo")
    new_foo_link = os.path.realpath("foo")

    assert system.is_symlink(os.path.join("data", "file"))
    new_data_link = os.path.realpath(os.path.join("data", "file"))

    assert relpath(old_foo_link, old_cache_dir) == relpath(
        new_foo_link, new_cache_dir
    )

    assert relpath(old_data_link, old_cache_dir) == relpath(
        new_data_link, new_cache_dir
    )


def test_checkout_no_checksum(tmp_dir, dvc):
    tmp_dir.gen("file", "file content")
    stage = dvc.run(
        outs=["file"], no_exec=True, cmd="somecmd", single_stage=True
    )

    with pytest.raises(CheckoutError):
        dvc.checkout([stage.path], force=True)

    assert not os.path.exists("file")


@pytest.mark.parametrize(
    "link, link_test_func",
    [("hardlink", system.is_hardlink), ("symlink", system.is_symlink)],
)
def test_checkout_relink(tmp_dir, dvc, link, link_test_func):
    dvc.odb.local.cache_types = [link]

    tmp_dir.dvc_gen({"dir": {"data": "text"}})
    dvc.unprotect("dir/data")
    assert not link_test_func("dir/data")

    stats = dvc.checkout(["dir.dvc"], relink=True)
    assert stats == empty_checkout
    assert link_test_func("dir/data")


@pytest.mark.parametrize("link", ["hardlink", "symlink", "copy"])
def test_checkout_relink_protected(tmp_dir, dvc, link):
    dvc.odb.local.cache_types = [link]

    tmp_dir.dvc_gen("foo", "foo")
    dvc.unprotect("foo")
    assert os.access("foo", os.W_OK)

    stats = dvc.checkout(["foo.dvc"], relink=True)
    assert stats == empty_checkout

    # NOTE: Windows symlink perms don't propagate to the target
    if link == "copy" or (link == "symlink" and os.name == "nt"):
        assert os.access("foo", os.W_OK)
    else:
        assert not os.access("foo", os.W_OK)


@pytest.mark.parametrize(
    "target",
    [os.path.join("dir", "subdir"), os.path.join("dir", "subdir", "file")],
)
def test_partial_checkout(tmp_dir, dvc, target):
    tmp_dir.dvc_gen({"dir": {"subdir": {"file": "file"}, "other": "other"}})
    shutil.rmtree("dir")
    stats = dvc.checkout([target])
    assert stats["added"] == ["dir" + os.sep]
    assert list(walk_files("dir")) == [os.path.join("dir", "subdir", "file")]


empty_checkout = {"added": [], "deleted": [], "modified": []}


def test_stats_on_empty_checkout(tmp_dir, dvc, scm):
    assert dvc.checkout() == empty_checkout
    tmp_dir.dvc_gen(
        {"dir": {"subdir": {"file": "file"}, "other": "other"}},
        commit="initial",
    )
    assert dvc.checkout() == empty_checkout


def test_stats_on_checkout(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen(
        {
            "dir": {"subdir": {"file": "file"}, "other": "other"},
            "foo": "foo",
            "bar": "bar",
        },
        commit="initial",
    )
    scm.checkout("HEAD~")
    stats = dvc.checkout()
    assert set(stats["deleted"]) == {"dir" + os.sep, "foo", "bar"}

    scm.checkout("-")
    stats = dvc.checkout()
    assert set(stats["added"]) == {"bar", "dir" + os.sep, "foo"}

    tmp_dir.gen({"lorem": "lorem", "bar": "new bar", "dir2": {"file": "file"}})
    (tmp_dir / "foo").unlink()
    scm.gitpython.repo.git.rm("foo.dvc")
    tmp_dir.dvc_add(["bar", "lorem", "dir2"], commit="second")

    scm.checkout("HEAD~")
    stats = dvc.checkout()
    assert set(stats["modified"]) == {"bar"}
    assert set(stats["added"]) == {"foo"}
    assert set(stats["deleted"]) == {"lorem", "dir2" + os.sep}

    scm.checkout("-")
    stats = dvc.checkout()
    assert set(stats["modified"]) == {"bar"}
    assert set(stats["added"]) == {"dir2" + os.sep, "lorem"}
    assert set(stats["deleted"]) == {"foo"}


@pytest.mark.xfail(reason="values relpath")
def test_checkout_stats_on_failure(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen(
        {"foo": "foo", "dir": {"subdir": {"file": "file"}}, "other": "other"},
        commit="initial",
    )
    stage = Dvcfile(dvc, "foo.dvc").stage
    tmp_dir.dvc_gen({"foo": "foobar", "other": "other other"}, commit="second")

    # corrupt cache
    cache = stage.outs[0].cache_path
    os.chmod(cache, 0o644)
    with open(cache, "a", encoding="utf-8") as fd:
        fd.write("destroy cache")

    scm.checkout("HEAD~")
    with pytest.raises(CheckoutError) as exc:
        dvc.checkout(force=True)

    assert exc.value.stats == {
        **empty_checkout,
        "failed": ["foo"],
        "modified": ["other"],
    }


def test_stats_on_added_file_from_tracked_dir(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen(
        {"dir": {"subdir": {"file": "file"}, "other": "other"}},
        commit="initial",
    )

    tmp_dir.gen("dir/subdir/newfile", "newfile")
    tmp_dir.dvc_add("dir", commit="add newfile")
    scm.checkout("HEAD~")
    assert dvc.checkout() == {**empty_checkout, "modified": ["dir" + os.sep]}
    assert dvc.checkout() == empty_checkout

    scm.checkout("-")
    assert dvc.checkout() == {**empty_checkout, "modified": ["dir" + os.sep]}
    assert dvc.checkout() == empty_checkout


def test_stats_on_updated_file_from_tracked_dir(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen(
        {"dir": {"subdir": {"file": "file"}, "other": "other"}},
        commit="initial",
    )

    tmp_dir.gen("dir/subdir/file", "what file?")
    tmp_dir.dvc_add("dir", commit="update file")
    scm.checkout("HEAD~")
    assert dvc.checkout() == {**empty_checkout, "modified": ["dir" + os.sep]}
    assert dvc.checkout() == empty_checkout

    scm.checkout("-")
    assert dvc.checkout() == {**empty_checkout, "modified": ["dir" + os.sep]}
    assert dvc.checkout() == empty_checkout


def test_stats_on_removed_file_from_tracked_dir(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen(
        {"dir": {"subdir": {"file": "file"}, "other": "other"}},
        commit="initial",
    )

    (tmp_dir / "dir" / "subdir" / "file").unlink()
    tmp_dir.dvc_add("dir", commit="removed file from subdir")
    scm.checkout("HEAD~")
    assert dvc.checkout() == {**empty_checkout, "modified": ["dir" + os.sep]}
    assert dvc.checkout() == empty_checkout

    scm.checkout("-")
    assert dvc.checkout() == {**empty_checkout, "modified": ["dir" + os.sep]}
    assert dvc.checkout() == empty_checkout


def test_stats_on_show_changes_does_not_show_summary(
    tmp_dir, dvc, scm, capsys
):
    tmp_dir.dvc_gen(
        {"dir": {"subdir": {"file": "file"}}, "other": "other"},
        commit="initial",
    )
    scm.checkout("HEAD~")

    assert main(["checkout"]) == 0

    out, _ = capsys.readouterr()
    assert out.splitlines() == [
        f"D\tdir{os.sep}".expandtabs(),
        "D\tother".expandtabs(),
    ]


def test_stats_does_not_show_changes_by_default(tmp_dir, dvc, scm, capsys):
    tmp_dir.dvc_gen(
        {"dir": {"subdir": {"file": "file"}}, "other": "other"},
        commit="initial",
    )
    scm.checkout("HEAD~")

    assert main(["checkout", "--summary"]) == 0

    out, _ = capsys.readouterr()
    assert "2 files deleted" == out.rstrip()


@pytest.mark.parametrize("link", ["hardlink", "symlink", "copy"])
def test_checkout_with_relink_existing(tmp_dir, dvc, link):
    tmp_dir.dvc_gen("foo", "foo")
    (tmp_dir / "foo").unlink()

    tmp_dir.dvc_gen("bar", "bar")
    dvc.odb.local.cache_types = [link]

    stats = dvc.checkout(relink=True)
    assert stats == {**empty_checkout, "added": ["foo"]}


def test_checkout_with_deps(tmp_dir, dvc):
    tmp_dir.dvc_gen({"foo": "foo"})
    dvc.run(
        fname="copy_file.dvc",
        cmd="echo foo > bar",
        outs=["bar"],
        deps=["foo"],
        single_stage=True,
    )

    (tmp_dir / "bar").unlink()
    (tmp_dir / "foo").unlink()

    stats = dvc.checkout(["copy_file.dvc"], with_deps=False)
    assert stats == {**empty_checkout, "added": ["bar"]}

    (tmp_dir / "bar").unlink()
    stats = dvc.checkout(["copy_file.dvc"], with_deps=True)
    assert set(stats["added"]) == {"foo", "bar"}


def test_checkout_recursive(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    dvc.add("dir", recursive=True)

    (tmp_dir / "dir" / "foo").unlink()
    (tmp_dir / "dir" / "bar").unlink()

    stats = dvc.checkout(["dir"], recursive=True)
    assert set(stats["added"]) == {
        os.path.join("dir", "foo"),
        os.path.join("dir", "bar"),
    }


def test_checkout_for_external_outputs(tmp_dir, dvc, workspace):
    workspace.gen("foo", "foo")

    file_path = workspace / "foo"
    dvc.add("remote://workspace/foo")

    odb = dvc.cloud.get_remote_odb("workspace")
    odb.fs.remove(str(file_path))
    assert not file_path.exists()

    stats = dvc.checkout(force=True)
    assert stats == {**empty_checkout, "added": ["remote://workspace/foo"]}
    assert file_path.exists()

    workspace.gen("foo", "foo\nfoo")

    stats = dvc.checkout(force=True)
    assert stats == {**empty_checkout, "modified": ["remote://workspace/foo"]}
    assert file_path.read_text() == "foo"


def test_checkouts_with_different_addressing(tmp_dir, dvc, run_copy):
    tmp_dir.gen({"foo": "foo", "lorem": "lorem"})
    run_copy("foo", "bar", name="copy-foo-bar")
    run_copy("lorem", "ipsum", name="copy-lorem-ipsum")

    (tmp_dir / "bar").unlink()
    (tmp_dir / "ipsum").unlink()
    assert set(dvc.checkout(PIPELINE_FILE)["added"]) == {"bar", "ipsum"}

    (tmp_dir / "bar").unlink()
    (tmp_dir / "ipsum").unlink()
    assert set(dvc.checkout(":")["added"]) == {"bar", "ipsum"}

    (tmp_dir / "bar").unlink()
    assert dvc.checkout("copy-foo-bar")["added"] == ["bar"]

    (tmp_dir / "bar").unlink()
    assert dvc.checkout("dvc.yaml:copy-foo-bar")["added"] == ["bar"]

    (tmp_dir / "bar").unlink()
    assert dvc.checkout(":copy-foo-bar")["added"] == ["bar"]

    (tmp_dir / "bar").unlink()
    (tmp_dir / "data").mkdir()
    with (tmp_dir / "data").chdir():
        assert dvc.checkout(relpath(tmp_dir / "dvc.yaml") + ":copy-foo-bar")[
            "added"
        ] == [relpath(tmp_dir / "bar")]

    (tmp_dir / "bar").unlink()
    assert dvc.checkout("bar")["added"] == ["bar"]


def test_checkouts_on_same_stage_name_and_output_name(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    run_copy("foo", "copy-foo-bar", name="make_collision")

    (tmp_dir / "bar").unlink()
    (tmp_dir / "copy-foo-bar").unlink()
    assert dvc.checkout("copy-foo-bar")["added"] == ["bar"]
    assert dvc.checkout("./copy-foo-bar")["added"] == ["copy-foo-bar"]


def test_checkouts_for_pipeline_tracked_outs(tmp_dir, dvc, scm, run_copy):
    tmp_dir.gen("foo", "foo")
    stage1 = run_copy("foo", "bar", name="copy-foo-bar")
    tmp_dir.gen("lorem", "lorem")
    stage2 = run_copy("lorem", "ipsum", name="copy-lorem-ipsum")

    for out in ["bar", "ipsum"]:
        (tmp_dir / out).unlink()
    assert dvc.checkout(["bar"])["added"] == ["bar"]

    (tmp_dir / "bar").unlink()
    assert set(dvc.checkout([PIPELINE_FILE])["added"]) == {"bar", "ipsum"}

    for out in ["bar", "ipsum"]:
        (tmp_dir / out).unlink()
    assert set(dvc.checkout([stage1.addressing])["added"]) == {"bar"}

    (tmp_dir / "bar").unlink()
    assert set(dvc.checkout([stage2.addressing])["added"]) == {"ipsum"}

    (tmp_dir / "ipsum").unlink()
    assert set(dvc.checkout()["added"]) == {"bar", "ipsum"}


def test_checkout_external_modified_file(tmp_dir, dvc, scm, mocker, workspace):
    # regression: happened when file in external output changed and checkout
    # was attempted without force, dvc checks if it's present in its cache
    # before asking user to remove it.
    workspace.gen("foo", "foo")
    dvc.add("remote://workspace/foo", external=True)
    scm.add(["foo.dvc"])
    scm.commit("add foo")

    workspace.gen("foo", "foobar")  # messing up the external outputs
    mocker.patch("dvc.prompt.confirm", return_value=True)
    dvc.checkout()

    assert (workspace / "foo").read_text() == "foo"


def test_checkout_executable(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")

    contents = (tmp_dir / "foo.dvc").parse()
    contents["outs"][0]["isexec"] = True
    (tmp_dir / "foo.dvc").dump(contents)

    dvc.checkout("foo")

    isexec = os.stat("foo").st_mode & stat.S_IEXEC
    if os.name == "nt":
        # NOTE: you can't set exec bits on Windows
        assert not isexec
    else:
        assert isexec


def test_checkout_partial(tmp_dir, dvc):
    tmp_dir.dvc_gen(
        {"data": {"foo": "foo", "bar": "bar", "sub_dir": {"baz": "baz"}}}
    )

    data_dir = tmp_dir / "data"
    shutil.rmtree(data_dir)

    dvc.checkout(str(data_dir / "foo"))
    assert data_dir.read_text() == {"foo": "foo"}

    dvc.checkout(str(data_dir / "sub_dir" / "baz"))
    assert data_dir.read_text() == {"foo": "foo", "sub_dir": {"baz": "baz"}}

    dvc.checkout(str(data_dir / "bar"))
    assert data_dir.read_text() == {
        "foo": "foo",
        "bar": "bar",
        "sub_dir": {"baz": "baz"},
    }


def test_checkout_partial_unchanged(tmp_dir, dvc):
    original_dir_shape = {
        "foo": "foo",
        "bar": "bar",
        "sub_dir": {"baz": "baz"},
        "empty_sub_dir": {},
    }
    tmp_dir.dvc_gen({"data": original_dir_shape})

    data_dir = tmp_dir / "data"
    sub_dir = data_dir / "sub_dir"
    foo = data_dir / "foo"
    bar = data_dir / "bar"
    sub_dir_file = sub_dir / "baz"

    # Nothing changed, nothing added/deleted/modified
    stats = dvc.checkout(str(bar))
    assert not any(stats.values())

    # Irrelevant file changed, still nothing added/deleted/modified
    foo.unlink()
    stats = dvc.checkout(str(bar))
    assert not any(stats.values())

    # Relevant change, one modified
    bar.unlink()
    stats = dvc.checkout(str(bar))
    assert len(stats["modified"]) == 1

    # No changes inside data/sub
    stats = dvc.checkout(str(sub_dir))
    assert not any(stats.values())

    # Relevant change, one modified
    sub_dir_file.unlink()
    stats = dvc.checkout(str(sub_dir))
    assert len(stats["modified"]) == 1

    stats = dvc.checkout(str(data_dir / "empty_sub_dir"))
    assert not any(stats.values())

    dvc.checkout(str(data_dir))

    # Everything is in place, no action taken
    stats = dvc.checkout(str(data_dir))
    assert not any(stats.values())


def test_checkout_partial_subdir(tmp_dir, dvc):
    tmp_dir.dvc_gen(
        {"data": {"foo": "foo", "sub_dir": {"bar": "bar", "baz": "baz"}}}
    )

    data_dir = tmp_dir / "data"
    sub_dir = data_dir / "sub_dir"
    sub_dir_bar = sub_dir / "baz"

    shutil.rmtree(sub_dir)
    dvc.checkout(str(sub_dir))
    assert data_dir.read_text() == {
        "foo": "foo",
        "sub_dir": {"bar": "bar", "baz": "baz"},
    }

    sub_dir_bar.unlink()
    dvc.checkout(str(sub_dir_bar))
    assert data_dir.read_text() == {
        "foo": "foo",
        "sub_dir": {"bar": "bar", "baz": "baz"},
    }


def test_checkout_file(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")

    stats = dvc.checkout("foo")
    assert not any(stats.values())

    os.unlink("foo")
    stats = dvc.checkout("foo")
    assert stats["added"] == ["foo"]


def test_checkout_dir_compat(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen({"data": {"foo": "foo"}})
    tmp_dir.gen(
        "data.dvc",
        textwrap.dedent(
            f"""\
        outs:
        - md5: {stage.outs[0].hash_info.value}
          path: data
        """
        ),
    )
    remove("data")
    dvc.checkout()
    assert (tmp_dir / "data").read_text() == {"foo": "foo"}

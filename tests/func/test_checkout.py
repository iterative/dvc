import logging
import os
import shutil
import stat
import textwrap

import pytest
from dulwich.porcelain import remove as git_rm

from dvc.cli import main
from dvc.dvcfile import PROJECT_FILE, FileMixin, SingleStageFile, load_file
from dvc.exceptions import CheckoutError, CheckoutErrorSuggestGit, NoOutputOrStageError
from dvc.fs import system
from dvc.stage.exceptions import StageFileDoesNotExistError
from dvc.utils import relpath
from dvc.utils.fs import remove
from tests.utils import get_gitignore_content

logger = logging.getLogger("dvc")


def walk_files(directory):
    for root, _, files in os.walk(directory):
        for f in files:
            yield os.path.join(root, f)


def test_checkout(tmp_dir, dvc, copy_script):
    tmp_dir.dvc_gen({"foo": "foo", "data": {"file": "file"}})
    dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="copy-foo-file1",
    )
    remove(tmp_dir / "foo")
    remove("data")

    assert dvc.checkout(force=True) == empty_checkout | {
        "added": ["data" + os.sep, "foo"],
        "stats": empty_stats | {"added": 2},
    }
    assert (tmp_dir / "foo").read_text() == "foo"
    assert (tmp_dir / "data").read_text() == {"file": "file"}


def test_checkout_cli(tmp_dir, dvc, copy_script):
    tmp_dir.dvc_gen({"foo": "foo", "data": {"file": "file"}})
    dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="copy-foo-file1",
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


@pytest.mark.parametrize(
    "summary,expected_lines",
    [
        (
            True,
            [
                "1 file modified, 3 files added and 1 file deleted",
            ],
        ),
        (
            False,
            [
                "M\tbar".expandtabs(),
                "A\tdir".expandtabs() + os.sep,
                "A\tfoo".expandtabs(),
                "A\tlorem".expandtabs(),
                "D\tipsum".expandtabs(),
            ],
        ),
    ],
)
def test_checkout_stats(tmp_dir, dvc, capsys, summary, expected_lines):
    tmp_dir.dvc_gen(
        {
            "foo": "foo",
            "bar": "bar",
            "lorem": "lorem",
            "dir": {"file": "file"},
            "ipsum": "ipsum",
            "dolor": "dolor",
        }
    )
    for out in ["foo", "bar", "lorem", "dir"]:
        remove(tmp_dir / out)
    (tmp_dir / "ipsum.dvc").unlink()
    (tmp_dir / "bar").write_text("foobar")

    opts = ["--summary"] if summary else []
    assert main(["checkout", "--force", *opts]) == 0
    out, _ = capsys.readouterr()
    assert out.splitlines() == expected_lines

    main(["checkout"])
    out, _ = capsys.readouterr()
    assert not out


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
    def test(self, mocker, tmp_dir, dvc):
        (stage,) = tmp_dir.dvc_gen("data", {"foo": "foo"})

        # change working directory
        (tmp_dir / "data").gen("not_cached.txt", "not_cached")
        assert main(["checkout", stage.relpath, "--force"]) == 0
        assert not (tmp_dir / "data" / "not_cached.txt").exists()

    def test_force(self, mocker, tmp_dir, dvc):
        (stage,) = tmp_dir.dvc_gen("data", {"foo": "foo"})

        # change working directory
        (tmp_dir / "data").gen("not_cached.txt", "not_cached")
        assert main(["checkout", stage.relpath]) != 0
        assert (tmp_dir / "data" / "not_cached.txt").exists()


def test_checkout_selective_remove(tmp_dir, dvc):
    # Use copy to test for changes in the inodes
    dvc.cache.local.cache_types = ["copy"]
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
        cmd="cp foo file3",
        deps=["foo"],
        outs_no_cache=["file3"],
        name="cp-foo-file3",
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


def test_checkout_missing_md5_in_lock_file_for_outs_deps(tmp_dir, dvc, copy_script):
    tmp_dir.dvc_gen({"foo": "foo", "data": {"file": "file"}})
    dvc.stage.add(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="copy-file",
    )

    with pytest.raises(CheckoutError) as exc:
        dvc.checkout(force=True)
    assert exc.value.result == empty_checkout | {"failed": ["file1"]}


def test_checkout_empty_dir(tmp_dir, dvc):
    empty_dir = tmp_dir / "empty_dir"
    empty_dir.mkdir()
    (stage,) = dvc.add("empty_dir")

    stage.outs[0].remove()
    assert not empty_dir.exists()

    assert dvc.checkout(force=True) == empty_checkout | {
        "added": [os.path.join("empty_dir", "")],
        "stats": empty_stats | {"added": 0},
    }
    assert empty_dir.is_dir()
    assert not list(empty_dir.iterdir())


def test_checkout_not_cached_file(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    dvc.run(cmd="cp foo bar", deps=["foo"], outs_no_cache=["bar"], name="copy-file")
    assert dvc.checkout(force=True) == empty_checkout


def test_checkout_with_deps_cli(tmp_dir, dvc, copy_script):
    tmp_dir.dvc_gen({"foo": "foo", "data": {"file": "file"}})
    dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="copy-file",
    )
    remove("foo")
    remove("file1")

    assert not os.path.exists("foo")
    assert not os.path.exists("file1")

    ret = main(["checkout", "--force", "copy-file", "--with-deps"])
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


def test_checkout_suggest_git(tmp_dir, dvc, scm):
    with pytest.raises(CheckoutErrorSuggestGit) as e:
        dvc.checkout(targets="gitbranch")
    assert isinstance(e.value.__cause__, NoOutputOrStageError)
    assert isinstance(e.value.__cause__.__cause__, StageFileDoesNotExistError)

    with pytest.raises(CheckoutErrorSuggestGit) as e:
        dvc.checkout(targets="foobar")
    assert isinstance(e.value.__cause__, NoOutputOrStageError)
    assert isinstance(e.value.__cause__.__cause__, StageFileDoesNotExistError)

    with pytest.raises(CheckoutErrorSuggestGit) as e:
        dvc.checkout(targets="looks-like-dvcfile.dvc")
    assert isinstance(e.value.__cause__, StageFileDoesNotExistError)
    assert e.value.__cause__.__cause__ is None


def test_checkout_target_recursive_should_not_remove_other_used_files(tmp_dir, dvc):
    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar", "data": {"file": "file"}})
    assert main(["checkout", "-R", "data"]) == 0
    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / "bar").exists()


def test_checkout_recursive_not_directory(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    ret = main(["add", "foo"])
    assert ret == 0

    assert dvc.checkout(targets=["foo.dvc"], recursive=True) == empty_checkout


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

    old_cache_dir = str(tmp_dir / ".dvc" / "cache")
    new_cache_dir = str(tmp_dir / ".dvc" / "cache_new")
    os.rename(old_cache_dir, new_cache_dir)

    ret = main(["cache", "dir", new_cache_dir])
    assert ret == 0

    ret = main(["checkout", "-f"])
    assert ret == 0

    assert system.is_symlink("foo")
    new_foo_link = os.path.realpath("foo")

    assert system.is_symlink(os.path.join("data", "file"))
    new_data_link = os.path.realpath(os.path.join("data", "file"))

    assert relpath(old_foo_link, old_cache_dir) == relpath(new_foo_link, new_cache_dir)

    assert relpath(old_data_link, old_cache_dir) == relpath(
        new_data_link, new_cache_dir
    )


def test_checkout_no_checksum(tmp_dir, dvc):
    tmp_dir.gen("file", "file content")
    dvc.run(outs=["file"], no_exec=True, cmd="somecmd", name="stage1")

    with pytest.raises(CheckoutError):
        dvc.checkout(["stage1"], force=True)

    assert not os.path.exists("file")


@pytest.mark.parametrize(
    "link, link_test_func",
    [("hardlink", system.is_hardlink), ("symlink", system.is_symlink)],
)
def test_checkout_relink(tmp_dir, dvc, link, link_test_func):
    dvc.cache.local.cache_types = [link]

    tmp_dir.dvc_gen({"dir": {"data": "text"}})

    data_file = os.path.join("dir", "data")

    # NOTE: Windows symlink perms don't propagate to the target
    if not (os.name == "nt" and link == "symlink"):
        assert not os.access(data_file, os.W_OK)

    dvc.unprotect(data_file)
    assert os.access(data_file, os.W_OK)
    assert not link_test_func(data_file)

    assert dvc.checkout(["dir.dvc"], relink=True) == empty_checkout
    assert link_test_func(data_file)

    # NOTE: Windows symlink perms don't propagate to the target and
    # hardlink was chmod-ed during relink to be deleted
    if not (os.name == "nt" and link in ["symlink", "hardlink"]):
        assert not os.access(data_file, os.W_OK)


@pytest.mark.parametrize(
    "target",
    [os.path.join("dir", "subdir"), os.path.join("dir", "subdir", "file")],
)
def test_partial_checkout(tmp_dir, dvc, target):
    tmp_dir.dvc_gen({"dir": {"subdir": {"file": "file"}, "other": "other"}})
    shutil.rmtree("dir")
    assert dvc.checkout([target]) == empty_checkout | {
        "added": ["dir" + os.sep],
        "stats": empty_stats | {"added": 1},
    }
    assert list(walk_files("dir")) == [os.path.join("dir", "subdir", "file")]


empty_stats = {"added": 0, "deleted": 0, "modified": 0}
empty_checkout = {"added": [], "deleted": [], "modified": [], "stats": empty_stats}


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
    assert stats == empty_checkout | {
        "deleted": ["dir" + os.sep, "bar", "foo"],
        "stats": empty_stats | {"deleted": 3},
    }

    scm.checkout("-")
    stats = dvc.checkout()
    assert stats == empty_checkout | {
        "added": ["dir" + os.sep, "bar", "foo"],
        "stats": empty_stats | {"added": 4},
    }

    tmp_dir.gen({"lorem": "lorem", "bar": "new bar", "dir2": {"file": "file"}})
    (tmp_dir / "foo").unlink()
    git_rm(tmp_dir, ["foo.dvc"])
    tmp_dir.dvc_add(["bar", "lorem", "dir2"], commit="second")

    scm.checkout("HEAD~")
    stats = dvc.checkout()
    assert stats == empty_checkout | {
        "modified": ["bar"],
        "added": ["foo"],
        "deleted": ["dir2" + os.sep, "lorem"],
        "stats": {"modified": 1, "added": 1, "deleted": 2},
    }

    scm.checkout("-")
    stats = dvc.checkout()
    assert stats == empty_checkout | {
        "modified": ["bar"],
        "added": ["dir2" + os.sep, "lorem"],
        "deleted": ["foo"],
        "stats": {"modified": 1, "added": 2, "deleted": 1},
    }


def test_checkout_stats_on_failure(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen(
        {"foo": "foo", "dir": {"subdir": {"file": "file"}}, "other": "other"},
        commit="initial",
    )
    stage = load_file(dvc, "foo.dvc").stage
    tmp_dir.dvc_gen({"foo": "foobar", "other": "other other"}, commit="second")

    # remove object from cache
    cache = stage.outs[0].cache_path
    remove(cache)

    (tmp_dir / "foo").unlink()

    scm.checkout("HEAD~")
    with pytest.raises(CheckoutError) as exc:
        dvc.checkout(force=True)

    assert exc.value.result == empty_checkout | {
        "failed": ["foo"],
        "modified": ["other"],
        "stats": empty_stats | {"modified": 1},
    }


def test_stats_on_added_file_from_tracked_dir(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen(
        {"dir": {"subdir": {"file": "file"}, "other": "other"}},
        commit="initial",
    )

    tmp_dir.gen("dir/subdir/newfile", "newfile")
    tmp_dir.dvc_add("dir", commit="add newfile")
    scm.checkout("HEAD~")
    assert dvc.checkout() == {
        **empty_checkout,
        "modified": ["dir" + os.sep],
        "stats": empty_stats | {"deleted": 1},
    }
    assert dvc.checkout() == empty_checkout

    scm.checkout("-")
    assert dvc.checkout() == {
        **empty_checkout,
        "modified": ["dir" + os.sep],
        "stats": empty_stats | {"added": 1},
    }
    assert dvc.checkout() == empty_checkout


def test_stats_on_updated_file_from_tracked_dir(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen(
        {"dir": {"subdir": {"file": "file"}, "other": "other"}},
        commit="initial",
    )

    tmp_dir.gen("dir/subdir/file", "what file?")
    tmp_dir.dvc_add("dir", commit="update file")
    scm.checkout("HEAD~")
    assert dvc.checkout() == {
        **empty_checkout,
        "modified": ["dir" + os.sep],
        "stats": empty_stats | {"modified": 1},
    }
    assert dvc.checkout() == empty_checkout

    scm.checkout("-")
    assert dvc.checkout() == {
        **empty_checkout,
        "modified": ["dir" + os.sep],
        "stats": empty_stats | {"modified": 1},
    }
    assert dvc.checkout() == empty_checkout


def test_stats_on_removed_file_from_tracked_dir(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen(
        {"dir": {"subdir": {"file": "file"}, "other": "other"}},
        commit="initial",
    )

    (tmp_dir / "dir" / "subdir" / "file").unlink()
    tmp_dir.dvc_add("dir", commit="removed file from subdir")
    scm.checkout("HEAD~")
    assert dvc.checkout() == {
        **empty_checkout,
        "modified": ["dir" + os.sep],
        "stats": empty_stats | {"added": 1},
    }
    assert dvc.checkout() == empty_checkout

    scm.checkout("-")
    assert dvc.checkout() == {
        **empty_checkout,
        "modified": ["dir" + os.sep],
        "stats": empty_stats | {"deleted": 2},
    }
    assert dvc.checkout() == empty_checkout


def test_stats_on_show_changes_does_not_show_summary(tmp_dir, dvc, scm, capsys):
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
    assert out.rstrip() == "2 files deleted"


@pytest.mark.parametrize("link", ["hardlink", "symlink", "copy"])
def test_checkout_with_relink_existing(tmp_dir, dvc, link):
    tmp_dir.dvc_gen("foo", "foo")
    (tmp_dir / "foo").unlink()

    tmp_dir.dvc_gen("bar", "bar")
    dvc.cache.local.cache_types = [link]

    stats = dvc.checkout(relink=True)
    assert stats == {
        **empty_checkout,
        "added": ["foo"],
        "stats": empty_stats | {"added": 1},
    }


def test_checkout_with_deps(tmp_dir, dvc):
    tmp_dir.dvc_gen({"foo": "foo"})
    dvc.run(cmd="echo foo > bar", outs=["bar"], deps=["foo"], name="copy-file")

    (tmp_dir / "bar").unlink()
    (tmp_dir / "foo").unlink()

    stats = dvc.checkout(["copy-file"], with_deps=False)
    assert stats == {
        **empty_checkout,
        "added": ["bar"],
        "stats": empty_stats | {"added": 1},
    }

    (tmp_dir / "bar").unlink()
    stats = dvc.checkout(["copy-file"], with_deps=True)
    assert stats == {
        **empty_checkout,
        "added": ["bar", "foo"],
        "stats": empty_stats | {"added": 2},
    }


def test_checkout_recursive(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    dvc.add("dir/*", glob=True)

    (tmp_dir / "dir" / "foo").unlink()
    (tmp_dir / "dir" / "bar").unlink()

    stats = dvc.checkout(["dir"], recursive=True)
    assert stats == empty_checkout | {
        "added": [os.path.join("dir", "bar"), os.path.join("dir", "foo")],
        "stats": empty_stats | {"added": 2},
    }


def test_checkouts_with_different_addressing(tmp_dir, dvc, run_copy):
    tmp_dir.gen({"foo": "foo", "lorem": "lorem"})
    run_copy("foo", "bar", name="copy-foo-bar")
    run_copy("lorem", "ipsum", name="copy-lorem-ipsum")

    (tmp_dir / "bar").unlink()
    (tmp_dir / "ipsum").unlink()
    assert dvc.checkout(PROJECT_FILE) == empty_checkout | {
        "added": ["bar", "ipsum"],
        "stats": empty_stats | {"added": 2},
    }

    (tmp_dir / "bar").unlink()
    (tmp_dir / "ipsum").unlink()
    assert dvc.checkout(":") == empty_checkout | {
        "added": ["bar", "ipsum"],
        "stats": empty_stats | {"added": 2},
    }

    (tmp_dir / "bar").unlink()
    assert dvc.checkout("copy-foo-bar") == empty_checkout | {
        "added": ["bar"],
        "stats": empty_stats | {"added": 1},
    }

    (tmp_dir / "bar").unlink()
    assert dvc.checkout("dvc.yaml:copy-foo-bar") == empty_checkout | {
        "added": ["bar"],
        "stats": empty_stats | {"added": 1},
    }

    (tmp_dir / "bar").unlink()
    assert dvc.checkout(":copy-foo-bar") == empty_checkout | {
        "added": ["bar"],
        "stats": empty_stats | {"added": 1},
    }

    (tmp_dir / "bar").unlink()
    (tmp_dir / "data").mkdir()
    with (tmp_dir / "data").chdir():
        assert dvc.checkout(
            relpath(tmp_dir / "dvc.yaml") + ":copy-foo-bar"
        ) == empty_checkout | {
            "added": [relpath(tmp_dir / "bar")],
            "stats": empty_stats | {"added": 1},
        }

    (tmp_dir / "bar").unlink()
    assert dvc.checkout("bar") == empty_checkout | {
        "added": ["bar"],
        "stats": empty_stats | {"added": 1},
    }


def test_checkouts_on_same_stage_name_and_output_name(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    run_copy("foo", "copy-foo-bar", name="make_collision")

    (tmp_dir / "bar").unlink()
    (tmp_dir / "copy-foo-bar").unlink()
    assert dvc.checkout("copy-foo-bar") == empty_checkout | {
        "added": ["bar"],
        "stats": empty_stats | {"added": 1},
    }
    assert dvc.checkout("./copy-foo-bar") == empty_checkout | {
        "added": ["copy-foo-bar"],
        "stats": empty_stats | {"added": 1},
    }


def test_checkouts_for_pipeline_tracked_outs(tmp_dir, dvc, scm, run_copy):
    tmp_dir.gen("foo", "foo")
    stage1 = run_copy("foo", "bar", name="copy-foo-bar")
    tmp_dir.gen("lorem", "lorem")
    stage2 = run_copy("lorem", "ipsum", name="copy-lorem-ipsum")

    for out in ["bar", "ipsum"]:
        (tmp_dir / out).unlink()
    assert dvc.checkout(["bar"]) == empty_checkout | {
        "added": ["bar"],
        "stats": empty_stats | {"added": 1},
    }

    (tmp_dir / "bar").unlink()
    assert dvc.checkout([PROJECT_FILE]) == empty_checkout | {
        "added": ["bar", "ipsum"],
        "stats": empty_stats | {"added": 2},
    }

    for out in ["bar", "ipsum"]:
        (tmp_dir / out).unlink()
    assert dvc.checkout([stage1.addressing]) == empty_checkout | {
        "added": ["bar"],
        "stats": empty_stats | {"added": 1},
    }

    (tmp_dir / "bar").unlink()
    assert dvc.checkout([stage2.addressing]) == empty_checkout | {
        "added": ["ipsum"],
        "stats": empty_stats | {"added": 1},
    }

    (tmp_dir / "ipsum").unlink()
    assert dvc.checkout() == empty_checkout | {
        "added": ["bar", "ipsum"],
        "stats": empty_stats | {"added": 2},
    }


def test_checkout_executable(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")

    contents = (tmp_dir / "foo.dvc").parse()
    contents["outs"][0]["isexec"] = True
    (tmp_dir / "foo.dvc").dump(contents)

    assert dvc.checkout("foo") == empty_checkout | {
        "modified": ["foo"],
        "stats": empty_stats | {"modified": 1},
    }

    isexec = os.stat("foo").st_mode & stat.S_IEXEC
    if os.name == "nt":
        # NOTE: you can't set exec bits on Windows
        assert not isexec
    else:
        assert isexec


def test_checkout_partial(tmp_dir, dvc):
    tmp_dir.dvc_gen({"data": {"foo": "foo", "bar": "bar", "sub_dir": {"baz": "baz"}}})

    data_dir = tmp_dir / "data"
    shutil.rmtree(data_dir)

    assert dvc.checkout(str(data_dir / "foo")) == empty_checkout | {
        "added": ["data" + os.sep],
        "stats": empty_stats | {"added": 1},
    }
    assert data_dir.read_text() == {"foo": "foo"}

    assert dvc.checkout(str(data_dir / "sub_dir" / "baz")) == empty_checkout | {
        "modified": ["data" + os.sep],
        "stats": empty_stats | {"added": 1},
    }
    assert data_dir.read_text() == {"foo": "foo", "sub_dir": {"baz": "baz"}}

    assert dvc.checkout(str(data_dir / "bar")) == empty_checkout | {
        "modified": ["data" + os.sep],
        "stats": empty_stats | {"added": 1},
    }
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
    assert dvc.checkout(str(bar)) == empty_checkout

    # Irrelevant file changed, still nothing added/deleted/modified
    foo.unlink()
    assert dvc.checkout(str(bar)) == empty_checkout

    # Relevant change, one modified
    bar.unlink()
    stats = dvc.checkout(str(bar))
    assert stats == empty_checkout | {
        "modified": ["data" + os.sep],
        "stats": empty_stats | {"added": 1},
    }

    # No changes inside data/sub
    assert dvc.checkout(str(sub_dir)) == empty_checkout

    # Relevant change, one modified
    sub_dir_file.unlink()
    stats = dvc.checkout(str(sub_dir))
    assert stats == empty_checkout | {
        "modified": ["data" + os.sep],
        "stats": empty_stats | {"added": 1},
    }
    assert len(stats["modified"]) == 1

    stats = dvc.checkout(str(data_dir / "empty_sub_dir"))
    assert stats == empty_checkout | {
        "modified": ["data" + os.sep],
        "stats": empty_stats | {"deleted": 1},
    }

    assert dvc.checkout(str(data_dir)) == empty_checkout | {
        "modified": ["data" + os.sep],
        "stats": empty_stats | {"added": 1},
    }

    # Everything is in place, no action taken
    assert dvc.checkout(str(data_dir)) == empty_checkout


def test_checkout_partial_subdir(tmp_dir, dvc):
    tmp_dir.dvc_gen({"data": {"foo": "foo", "sub_dir": {"bar": "bar", "baz": "baz"}}})

    data_dir = tmp_dir / "data"
    sub_dir = data_dir / "sub_dir"
    sub_dir_bar = sub_dir / "baz"

    shutil.rmtree(sub_dir)
    assert dvc.checkout(str(sub_dir)) == empty_checkout | {
        "modified": ["data" + os.sep],
        "stats": empty_stats | {"added": 2},
    }
    assert data_dir.read_text() == {
        "foo": "foo",
        "sub_dir": {"bar": "bar", "baz": "baz"},
    }

    sub_dir_bar.unlink()
    assert dvc.checkout(str(sub_dir_bar)) == empty_checkout | {
        "modified": ["data" + os.sep],
        "stats": empty_stats | {"added": 1},
    }
    assert data_dir.read_text() == {
        "foo": "foo",
        "sub_dir": {"bar": "bar", "baz": "baz"},
    }


def test_checkout_file(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")

    assert dvc.checkout("foo") == empty_checkout

    os.unlink("foo")
    stats = dvc.checkout("foo")
    assert stats == empty_checkout | {
        "added": ["foo"],
        "stats": empty_stats | {"added": 1},
    }


def test_checkout_dir_compat(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen({"data": {"foo": "foo"}})
    tmp_dir.gen(
        "data.dvc",
        textwrap.dedent(
            f"""\
        outs:
        - md5: {stage.outs[0].hash_info.value}
          hash: md5
          path: data
        """
        ),
    )
    remove("data")
    assert dvc.checkout() == empty_checkout | {
        "added": ["data" + os.sep],
        "stats": empty_stats | {"added": 1},
    }
    assert (tmp_dir / "data").read_text() == {"foo": "foo"}


def test_checkout_cleanup_properly_on_untracked_nested_directories(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"datasets": {"dir1": {"file1": "file1"}}})
    tmp_dir.gen({"datasets": {"dir2": {"dir3": {"file2": "file2"}}}})

    assert dvc.checkout(force=True) == empty_checkout | {
        "modified": ["datasets" + os.sep],
        "stats": empty_stats | {"deleted": 3},
    }

    assert (tmp_dir / "datasets").read_text() == {"dir1": {"file1": "file1"}}


def test_checkout_loads_specific_file(tmp_dir, dvc, mocker):
    tmp_dir.dvc_gen("foo", "foo")
    tmp_dir.dvc_gen("bar", "bar")

    (tmp_dir / "bar").unlink()
    (tmp_dir / "foo").unlink()

    f = SingleStageFile(dvc, "foo.dvc")

    spy = mocker.spy(FileMixin, "_load")
    assert dvc.checkout("foo.dvc") == empty_checkout | {
        "added": ["foo"],
        "stats": empty_stats | {"added": 1},
    }

    spy.assert_called_with(f)
    assert (tmp_dir / "foo").exists()
    assert not (tmp_dir / "bar").exists()

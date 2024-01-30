import errno
import filecmp
import os
import shutil
import stat
import textwrap

import pytest

import dvc_data
from dvc.cachemgr import CacheManager
from dvc.cli import main
from dvc.config import ConfigError
from dvc.dvcfile import DVC_FILE_SUFFIX
from dvc.exceptions import (
    DvcException,
    OutputDuplicationError,
    OverlappingOutputPathsError,
)
from dvc.fs import LocalFileSystem, system
from dvc.output import (
    OutputAlreadyTrackedError,
    OutputDoesNotExistError,
    OutputIsStageFileError,
)
from dvc.stage import Stage
from dvc.stage.exceptions import StageExternalOutputsError, StagePathNotFoundError
from dvc.utils.fs import path_isin
from dvc.utils.serialize import YAMLFileCorruptedError, dump_yaml
from dvc_data.hashfile.hash import file_md5
from dvc_data.hashfile.hash_info import HashInfo
from tests.utils import get_gitignore_content


def test_add(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    (stage,) = dvc.add("foo")
    md5 = file_md5("foo", dvc.fs)

    assert stage is not None

    assert isinstance(stage, Stage)
    assert os.path.isfile(stage.path)
    assert len(stage.outs) == 1
    assert len(stage.deps) == 0
    assert stage.cmd is None
    assert stage.outs[0].hash_info == HashInfo("md5", md5)
    assert stage.md5 is None

    assert (tmp_dir / "foo.dvc").parse() == {
        "outs": [
            {
                "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                "path": "foo",
                "size": 3,
                "hash": "md5",
            }
        ]
    }


@pytest.mark.skipif(os.name == "nt", reason="can't set exec bit on Windows")
def test_add_executable(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    st = os.stat("foo")
    os.chmod("foo", st.st_mode | stat.S_IEXEC)
    dvc.add("foo")

    assert (tmp_dir / "foo.dvc").parse() == {
        "outs": [
            {
                "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                "path": "foo",
                "size": 3,
                "isexec": True,
                "hash": "md5",
            }
        ]
    }
    assert os.stat("foo").st_mode & stat.S_IEXEC


def test_add_unicode(tmp_dir, dvc):
    with open("\xe1", "wb", encoding=None) as fd:
        fd.write(b"something")

    (stage,) = dvc.add("\xe1")

    assert os.path.isfile(stage.path)


def test_add_unsupported_file(dvc):
    with pytest.raises(ConfigError, match="Unsupported URL type"):
        dvc.add("unsupported://unsupported")


def test_add_directory(tmp_dir, dvc):
    from dvc_data.hashfile import load

    (stage,) = tmp_dir.dvc_gen({"dir": {"file": "file"}})

    assert stage is not None
    assert len(stage.deps) == 0
    assert len(stage.outs) == 1

    hash_info = stage.outs[0].hash_info

    obj = load(dvc.cache.local, hash_info)
    for key, _, _ in obj:
        for part in key:
            assert "\\" not in part


def test_add_directory_with_forward_slash(tmp_dir, dvc):
    tmp_dir.gen("directory", {"file": "file"})
    (stage,) = dvc.add("directory/")
    assert stage.relpath == "directory.dvc"


def test_add_tracked_file(tmp_dir, scm, dvc):
    path = "tracked_file"
    tmp_dir.scm_gen(path, "...", commit="add tracked file")
    msg = f""" output '{path}' is already tracked by SCM \\(e.g. Git\\).
    You can remove it from Git, then add to DVC.
        To stop tracking from Git:
            git rm -r --cached '{path}'
            git commit -m "stop tracking {path}" """

    with pytest.raises(OutputAlreadyTrackedError, match=msg):
        dvc.add(path)


def test_add_dir_with_existing_cache(tmp_dir, dvc):
    tmp_dir.gen({"foo": "foo", "dir": {"file": "foo"}})

    (stage,) = dvc.add("foo")
    assert stage is not None
    (stage,) = dvc.add("dir")
    assert stage is not None


def test_add_modified_dir(tmp_dir, dvc):
    tmp_dir.gen("data", {"foo": "foo", "sub": {"bar": "bar"}})
    (stage,) = dvc.add("data")
    assert stage is not None

    (tmp_dir / "data" / "foo").unlink()
    (stage,) = dvc.add("data")
    assert stage is not None


def test_add_file_in_dir(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"subdir": {"subdata": "subdata content"}}})
    subdir_path = os.path.join("dir", "subdir", "subdata")

    (stage,) = dvc.add(subdir_path)

    assert stage is not None
    assert len(stage.deps) == 0
    assert len(stage.outs) == 1
    assert stage.relpath == subdir_path + ".dvc"

    # Current dir should not be taken into account
    assert stage.wdir == os.path.dirname(stage.path)
    assert stage.outs[0].def_path == "subdata"


@pytest.mark.parametrize(
    "target, expected_def_paths, expected_rel_paths",
    [
        (
            os.path.join("dir", "subdir", "subdata*"),
            ["subdata", "subdata123"],
            [
                os.path.join("dir", "subdir", "subdata") + ".dvc",
                os.path.join("dir", "subdir", "subdata123") + ".dvc",
            ],
        ),
        (
            os.path.join("dir", "subdir", "?subdata"),
            ["esubdata", "isubdata"],
            [
                os.path.join("dir", "subdir", "esubdata") + ".dvc",
                os.path.join("dir", "subdir", "isubdata") + ".dvc",
            ],
        ),
        (
            os.path.join("dir", "subdir", "[aiou]subdata"),
            ["isubdata"],
            [os.path.join("dir", "subdir", "isubdata") + ".dvc"],
        ),
        (
            os.path.join("dir", "**", "subdata*"),
            ["subdata", "subdata123", "subdata4", "subdata5"],
            [
                os.path.join("dir", "subdir", "subdata") + ".dvc",
                os.path.join("dir", "subdir", "subdata123") + ".dvc",
                os.path.join("dir", "anotherdir", "subdata4") + ".dvc",
                os.path.join("dir", "subdata5") + ".dvc",
            ],
        ),
    ],
)
def test_add_filtered_files_in_dir(
    tmp_dir, dvc, target, expected_def_paths, expected_rel_paths
):
    tmp_dir.gen(
        {
            "dir": {
                "subdir": {
                    "subdata": "subdata content",
                    "esubdata": "extra subdata content",
                    "isubdata": "i subdata content",
                    "subdata123": "subdata content 123",
                },
                "anotherdir": {
                    "subdata4": "subdata 4 content",
                    "esubdata": "extra 2 subdata content",
                },
                "subdata5": "subdata 5 content",
            }
        }
    )

    stages = dvc.add(target, glob=True)

    assert len(stages) == len(expected_def_paths)
    for stage in stages:
        assert stage is not None
        assert len(stage.deps) == 0
        assert len(stage.outs) == 1
        assert stage.relpath in expected_rel_paths

        # Current dir should not be taken into account
        assert stage.wdir == os.path.dirname(stage.path)
        assert stage.outs[0].def_path in expected_def_paths


def test_cmd_add(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    ret = main(["add", "foo"])
    assert ret == 0

    ret = main(["add", "non-existing-file"])
    assert ret != 0


def test_double_add_unchanged_file(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    ret = main(["add", "foo"])
    assert ret == 0

    ret = main(["add", "foo"])
    assert ret == 0


def test_double_add_unchanged_dir(tmp_dir, dvc):
    tmp_dir.gen("data", {"foo": "foo"})
    ret = main(["add", "data"])
    assert ret == 0

    ret = main(["add", "data"])
    assert ret == 0


@pytest.mark.skipif(os.name == "nt", reason="unsupported on Windows")
def test_add_colon_in_filename(tmp_dir, dvc):
    tmp_dir.gen("fo:o", "foo")
    ret = main(["add", "fo:o"])
    assert ret == 0


def test_should_update_state_entry_for_file_after_add(mocker, dvc, tmp_dir):
    file_md5_counter = mocker.spy(dvc_data.hashfile.hash, "file_md5")
    tmp_dir.gen("foo", "foo")

    ret = main(["config", "cache.type", "copy"])
    assert ret == 0

    ret = main(["add", "foo"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 1

    ret = main(["status"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 1

    os.rename("foo", "foo.back")
    ret = main(["checkout"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 2

    ret = main(["status"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 2


def test_should_update_state_entry_for_directory_after_add(mocker, dvc, tmp_dir):
    file_md5_counter = mocker.spy(dvc_data.hashfile.hash, "file_md5")

    tmp_dir.gen({"data/data": "foo", "data/data_sub/sub_data": "foo"})

    ret = main(["config", "cache.type", "copy"])
    assert ret == 0

    ret = main(["add", "data"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 4

    ret = main(["status"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 5

    os.rename("data", "data.back")
    ret = main(["checkout"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 6

    ret = main(["status"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 7


def test_add_commit(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    ret = main(["add", "foo", "--no-commit"])
    assert ret == 0
    assert os.path.isfile("foo")
    assert not os.path.exists(dvc.cache.local.path)

    ret = main(["commit", "foo.dvc"])
    assert ret == 0
    assert os.path.isfile("foo")
    assert dvc.cache.local.exists("acbd18db4cc2f85cedef654fccc4a4d8")


def test_should_collect_dir_cache_only_once(mocker, tmp_dir, dvc):
    tmp_dir.gen({"data/data": "foo"})
    counter = mocker.spy(dvc_data.hashfile.build, "_build_tree")
    ret = main(["add", "data"])
    assert ret == 0
    assert counter.mock.call_count == 2

    ret = main(["status"])
    assert ret == 0
    assert counter.mock.call_count == 3

    ret = main(["status"])
    assert ret == 0
    assert counter.mock.call_count == 4


def test_should_place_stage_in_data_dir_if_repository_below_symlink(
    mocker, tmp_dir, dvc
):
    def is_symlink_true_below_dvc_root(path):
        return path == os.path.dirname(dvc.root_dir)

    tmp_dir.gen({"data": {"foo": "foo"}})
    mocker.patch.object(
        system, "is_symlink", side_effect=is_symlink_true_below_dvc_root
    )
    ret = main(["add", os.path.join("data", "foo")])
    assert ret == 0

    assert not (tmp_dir / "foo.dvc").exists()
    assert (tmp_dir / "data" / "foo.dvc").exists()


def test_should_throw_proper_exception_on_corrupted_stage_file(caplog, tmp_dir, dvc):
    tmp_dir.gen({"foo": "foo", "bar": " bar"})
    assert main(["add", "foo"]) == 0

    with (tmp_dir / "foo.dvc").open("a+") as f:
        f.write("this will break yaml file structure")

    caplog.clear()
    assert main(["add", "bar"]) == 1
    expected_error = "unable to read: 'foo.dvc', YAML file structure is corrupted"
    assert expected_error in caplog.text


def test_should_throw_proper_exception_on_existing_out(caplog, tmp_dir, dvc):
    tmp_dir.gen({"foo": "foo"})
    (tmp_dir / "out").write_text("old contents")

    assert main(["add", "foo", "--out", "out"]) == 1

    assert (tmp_dir / "out").read_text() == "old contents"
    expected_error_lines = [
        "Error: The file 'out' already exists locally.",
        "To override it, re-run with '--force'.",
    ]
    assert all(line in caplog.text for line in expected_error_lines)


def test_add_force_overwrite_out(caplog, tmp_dir, dvc):
    tmp_dir.gen({"foo": "foo"})
    (tmp_dir / "out").write_text("old contents")

    assert main(["add", "foo", "--out", "out", "--force"]) == 0
    assert (tmp_dir / "foo").read_text() == "foo"


def test_failed_add_cleanup(tmp_dir, scm, dvc):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})

    # Add and corrupt a stage file
    dvc.add("foo")
    tmp_dir.gen("foo.dvc", "- broken\nyaml")

    with pytest.raises(YAMLFileCorruptedError):
        dvc.add("bar")

    assert not os.path.exists("bar.dvc")

    gitignore_content = get_gitignore_content()
    assert "/bar" not in gitignore_content


def test_add_unprotected(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    ret = main(["config", "cache.type", "hardlink"])
    assert ret == 0

    ret = main(["add", "foo"])
    assert ret == 0

    assert not os.access("foo", os.W_OK)
    assert system.is_hardlink("foo")

    ret = main(["unprotect", "foo"])
    assert ret == 0

    ret = main(["add", "foo"])
    assert ret == 0

    assert not os.access("foo", os.W_OK)
    assert system.is_hardlink("foo")


@pytest.fixture
def temporary_windows_drive(tmp_path_factory):
    import string
    from ctypes import windll

    try:
        import win32api
        from win32con import DDD_REMOVE_DEFINITION
    except ImportError:
        pytest.skip("pywin32 not installed")

    drives = [
        s[0].upper()
        for s in win32api.GetLogicalDriveStrings().split("\000")
        if len(s) > 0
    ]

    new_drive_name = next(
        letter for letter in string.ascii_uppercase if letter not in drives
    )
    new_drive = f"{new_drive_name}:"

    target_path = tmp_path_factory.mktemp("tmp_windows_drive")

    set_up_result = windll.kernel32.DefineDosDeviceW(
        0, new_drive, os.fspath(target_path)
    )
    if set_up_result == 0:
        raise RuntimeError("Failed to mount windows drive!")

    # NOTE: new_drive has form of `A:` and joining it with some relative
    # path might result in non-existing path (A:path\\to)
    yield os.path.join(new_drive, os.sep)

    tear_down_result = windll.kernel32.DefineDosDeviceW(
        DDD_REMOVE_DEFINITION, new_drive, os.fspath(target_path)
    )
    if tear_down_result == 0:
        raise RuntimeError("Could not unmount windows drive!")


@pytest.mark.skipif(os.name != "nt", reason="Windows specific")
def test_windows_should_add_when_cache_on_different_drive(
    tmp_dir, dvc, temporary_windows_drive
):
    dvc.config["cache"]["dir"] = temporary_windows_drive
    dvc.cache = CacheManager(dvc)

    (stage,) = tmp_dir.dvc_gen({"file": "file"})
    cache_path = stage.outs[0].cache_path

    assert path_isin(cache_path, temporary_windows_drive)
    assert os.path.isfile(cache_path)
    filecmp.cmp("file", cache_path)


def test_readding_dir_should_not_unprotect_all(tmp_dir, dvc, mocker):
    tmp_dir.gen("dir/data", "data")

    dvc.cache.local.cache_types = ["symlink"]

    dvc.add("dir")
    tmp_dir.gen("dir/new_file", "new_file_content")

    unprotect_spy = mocker.spy(dvc.cache.local, "unprotect")
    dvc.add("dir")

    assert not unprotect_spy.mock.called
    assert system.is_symlink(os.path.join("dir", "new_file"))


def test_should_not_checkout_when_adding_cached_copy(tmp_dir, dvc, mocker):
    dvc.cache.local.cache_types = ["copy"]

    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})

    shutil.copy("bar", "foo")

    copy_spy = mocker.spy(dvc.cache.local.fs, "copy")

    dvc.add("foo")

    assert copy_spy.mock.call_count == 0


@pytest.mark.parametrize(
    "link,new_link,link_test_func",
    [
        ("hardlink", "copy", lambda path: not system.is_hardlink(path)),
        ("symlink", "copy", lambda path: not system.is_symlink(path)),
        ("copy", "hardlink", system.is_hardlink),
        ("copy", "symlink", system.is_symlink),
    ],
)
def test_should_relink_on_repeated_add(link, new_link, link_test_func, tmp_dir, dvc):
    dvc.config["cache"]["type"] = link

    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})

    os.remove("foo")
    getattr(dvc.cache.local.fs, link)(
        (tmp_dir / "bar").fs_path, (tmp_dir / "foo").fs_path
    )

    dvc.cache.local.cache_types = [new_link]

    dvc.add("foo")

    assert link_test_func("foo")


@pytest.mark.parametrize("link", ["hardlink", "symlink", "copy"])
def test_should_protect_on_repeated_add(link, tmp_dir, dvc):
    dvc.cache.local.cache_types = [link]

    tmp_dir.dvc_gen({"foo": "foo"})

    dvc.unprotect("foo")

    dvc.add("foo")

    assert not os.access(
        os.path.join(".dvc", "cache", "ac", "bd18db4cc2f85cedef654fccc4a4d8"),
        os.W_OK,
    )

    # NOTE: Windows symlink perms don't propagate to the target
    if link == "copy" or (link == "symlink" and os.name == "nt"):
        assert os.access("foo", os.W_OK)
    else:
        assert not os.access("foo", os.W_OK)


def test_escape_gitignore_entries(tmp_dir, scm, dvc):
    fname = "file!with*weird#naming_[1].t?t"
    ignored_fname = r"/file\!with\*weird\#naming_\[1\].t\?t"

    if os.name == "nt":
        # Some characters are not supported by Windows in the filename
        # https://docs.microsoft.com/en-us/windows/win32/fileio/naming-a-file
        fname = "file!with_weird#naming_[1].txt"
        ignored_fname = r"/file\!with_weird\#naming_\[1\].txt"

    tmp_dir.dvc_gen(fname, "...")
    assert ignored_fname in get_gitignore_content()


def test_add_from_data_dir(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"dir": {"file1": "file1 content"}})

    tmp_dir.gen({"dir": {"file2": "file2 content"}})

    dvc.add(os.path.join("dir", "file2"))


def test_add_parent_dir(tmp_dir, scm, dvc):
    tmp_dir.gen({"dir": {"file1": "file1 content"}})
    out_path = os.path.join("dir", "file1")
    dvc.add(out_path)

    with pytest.raises(OverlappingOutputPathsError) as e:
        dvc.add("dir")
    assert str(e.value) == (
        "Cannot add 'dir', because it is overlapping with other DVC "
        "tracked output: '{out}'.\n"
        "To include '{out}' in 'dir', run 'dvc remove {out}.dvc' "
        "and then 'dvc add dir'"
    ).format(out=os.path.join("dir", "file1"))


def test_not_raises_on_re_add(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file content")

    tmp_dir.gen({"file2": "file2 content", "file": "modified file"})
    dvc.add(["file2", "file"])


@pytest.mark.parametrize("link", ["hardlink", "symlink", "copy"])
def test_add_empty_files(tmp_dir, dvc, link):
    file = "foo"
    dvc.cache.local.cache_types = [link]
    stages = tmp_dir.dvc_gen(file, "")

    assert (tmp_dir / file).exists()
    assert (tmp_dir / (file + DVC_FILE_SUFFIX)).exists()
    assert os.path.exists(stages[0].outs[0].cache_path)


def test_add_optimization_for_hardlink_on_empty_files(tmp_dir, dvc, mocker):
    dvc.cache.local.cache_types = ["hardlink"]
    tmp_dir.gen({"foo": "", "bar": "", "lorem": "lorem", "ipsum": "ipsum"})
    m = mocker.spy(LocalFileSystem, "is_hardlink")
    stages = dvc.add(["foo", "bar", "lorem", "ipsum"])

    assert m.call_count == 8
    assert m.call_args != mocker.call(tmp_dir / "foo")
    assert m.call_args != mocker.call(tmp_dir / "bar")

    for stage in stages[:2]:
        # hardlinks are not created for empty files
        assert not system.is_hardlink(stage.outs[0].fs_path)

    for stage in stages[2:]:
        assert system.is_hardlink(stage.outs[0].fs_path)

    for stage in stages:
        assert os.path.exists(stage.path)
        assert os.path.exists(stage.outs[0].cache_path)


def test_try_adding_pipeline_tracked_output(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    msg = (
        "cannot update 'bar': overlaps with an output of stage: 'copy-foo-bar' in "
        "'dvc.yaml'.\nRun the pipeline or use 'dvc commit' to force update it."
    )
    with pytest.raises(DvcException, match=msg):
        dvc.add("bar")


def test_try_adding_multiple_overlaps(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    dvcyaml_content = {
        "stages": {
            "echo-foo": {
                "cmd": "echo foo > foo",
                "outs": ["foo"],
            }
        }
    }
    dump_yaml("dvc.yaml", dvcyaml_content)
    msg = (
        "\nUse `dvc remove` with any of the above targets to stop tracking the "
        "overlapping output."
    )
    with pytest.raises(OutputDuplicationError, match=msg):
        dvc.add("foo")


def test_add_pipeline_file(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import PROJECT_FILE

    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")

    with pytest.raises(OutputIsStageFileError):
        dvc.add(PROJECT_FILE)


def test_add_symlink_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"bar": "bar"}})

    (tmp_dir / "dir" / "foo").symlink_to(os.path.join(".", "bar"))

    dvc.add(os.path.join("dir", "foo"))

    assert not (tmp_dir / "foo.dvc").exists()
    assert (tmp_dir / "dir" / "foo.dvc").exists()
    assert not (tmp_dir / "dir" / "foo").is_symlink()
    assert not (tmp_dir / "dir" / "bar").is_symlink()
    assert (tmp_dir / "dir" / "foo").read_text() == "bar"
    assert (tmp_dir / "dir" / "bar").read_text() == "bar"

    assert (
        tmp_dir
        / ".dvc"
        / "cache"
        / "files"
        / "md5"
        / "37"
        / "b51d194a7513e45b56f6524f2d51f2"
    ).read_text() == "bar"
    assert not (
        tmp_dir
        / ".dvc"
        / "cache"
        / "files"
        / "md5"
        / "37"
        / "b51d194a7513e45b56f6524f2d51f2"
    ).is_symlink()

    # Test that subsequent add succeeds
    # See https://github.com/iterative/dvc/issues/4654
    dvc.add(os.path.join("dir", "foo"))


def test_add_symlink_dir(make_tmp_dir, tmp_dir, dvc):
    tmp_dir.gen({"data": {"foo": "foo"}})
    target = os.path.join(".", "data")

    tmp_dir.gen({"data": {"foo": "foo"}})

    (tmp_dir / "dir").symlink_to(target)

    msg = "Cannot add files inside symlinked directories to DVC"
    with pytest.raises(DvcException, match=msg):
        dvc.add("dir")


def test_add_file_in_symlink_dir(make_tmp_dir, tmp_dir, dvc):
    tmp_dir.gen({"data": {"foo": "foo"}})
    target = os.path.join(".", "data")

    (tmp_dir / "dir").symlink_to(target)

    msg = "Cannot add files inside symlinked directories to DVC"
    with pytest.raises(DvcException, match=msg):
        dvc.add(os.path.join("dir", "foo"))


def test_add_with_cache_link_error(tmp_dir, dvc, mocker, capsys):
    tmp_dir.gen("foo", "foo")

    mocker.patch("dvc_data.hashfile.checkout.test_links", return_value=[])
    dvc.add("foo")
    err = capsys.readouterr()[1]
    assert "reconfigure cache types" in err

    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / "foo.dvc").exists()
    assert (
        tmp_dir
        / ".dvc"
        / "cache"
        / "files"
        / "md5"
        / "ac"
        / "bd18db4cc2f85cedef654fccc4a4d8"
    ).read_text() == "foo"


def test_add_preserve_fields(tmp_dir, dvc):
    text = textwrap.dedent(
        """\
        # top comment
        desc: top desc
        outs:
        - path: foo # out comment
          desc: out desc
          type: mytype
          labels:
          - label1
          - label2
          remote: testremote
        meta: some metadata
    """
    )
    tmp_dir.gen("foo.dvc", text)
    tmp_dir.dvc_gen("foo", "foo")
    assert (tmp_dir / "foo.dvc").read_text() == textwrap.dedent(
        """\
        # top comment
        desc: top desc
        outs:
        - path: foo # out comment
          desc: out desc
          type: mytype
          labels:
          - label1
          - label2
          remote: testremote
          md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
          hash: md5
        meta: some metadata
    """
    )


# NOTE: unless long paths are enabled on Windows, PATH_MAX and NAME_MAX
# are the same 260 chars, which makes the test unnecessarily complex
@pytest.mark.skipif(os.name == "nt", reason="unsupported on Windows")
def test_add_long_fname(tmp_dir, dvc):
    name_max = os.pathconf(tmp_dir, "PC_NAME_MAX")
    name = "a" * name_max
    tmp_dir.gen({"data": {name: "foo"}})

    # nothing we can do in this case, as the resulting dvcfile
    # will definitely exceed NAME_MAX
    with pytest.raises(OSError, match=f"File name too long: .*{name}") as info:
        dvc.add(os.path.join("data", name))
    assert info.value.errno == errno.ENAMETOOLONG

    dvc.add("data")
    assert (tmp_dir / "data").read_text() == {name: "foo"}


def test_add_to_remote_absolute(tmp_dir, make_tmp_dir, dvc, remote):
    tmp_abs_dir = make_tmp_dir("abs")
    tmp_foo = tmp_abs_dir / "foo"
    tmp_foo.write_text("foo")

    dvc.add(str(tmp_foo), to_remote=True)
    tmp_foo.unlink()

    foo = tmp_dir / "foo"
    assert foo.with_suffix(".dvc").exists()
    assert not os.path.exists(tmp_foo)

    dvc.pull("foo")
    assert not os.path.exists(tmp_foo)
    assert foo.read_text() == "foo"

    tmp_bar = tmp_abs_dir / "bar"
    with pytest.raises(StageExternalOutputsError):
        dvc.add(str(tmp_foo), out=str(tmp_bar), to_remote=True)


def test_add_to_cache_dir(tmp_dir, dvc, local_cloud):
    local_cloud.gen({"data": {"foo": "foo", "bar": "bar"}})

    (stage,) = dvc.add(str(local_cloud / "data"), out="data")
    assert len(stage.deps) == 0
    assert len(stage.outs) == 1
    assert stage.outs[0].meta.size == len("foo") + len("bar")
    assert stage.outs[0].meta.nfiles == 2

    data = tmp_dir / "data"
    assert data.read_text() == {"foo": "foo", "bar": "bar"}
    assert (tmp_dir / "data.dvc").exists()

    shutil.rmtree(data)
    status = dvc.checkout(str(data))
    assert status["added"] == ["data" + os.sep]
    assert data.read_text() == {"foo": "foo", "bar": "bar"}


def test_add_to_cache_file(tmp_dir, dvc, local_cloud):
    local_cloud.gen("foo", "foo")

    (stage,) = dvc.add(str(local_cloud / "foo"), out="foo")
    assert len(stage.deps) == 0
    assert len(stage.outs) == 1

    foo = tmp_dir / "foo"
    assert foo.read_text() == "foo"
    assert (tmp_dir / "foo.dvc").exists()

    foo.unlink()
    status = dvc.checkout(str(foo))
    assert status["added"] == ["foo"]
    assert foo.read_text() == "foo"


def test_add_with_out(tmp_dir, scm, dvc):
    tmp_dir.gen({"foo": "foo"})
    dvc.add("foo", out="out_foo")
    gitignore_content = get_gitignore_content()
    assert "/out_foo" in gitignore_content


def test_add_to_cache_different_name(tmp_dir, dvc, local_cloud):
    local_cloud.gen({"data": {"foo": "foo", "bar": "bar"}})

    dvc.add(str(local_cloud / "data"), out="not_data")

    not_data = tmp_dir / "not_data"
    assert not_data.read_text() == {"foo": "foo", "bar": "bar"}
    assert (tmp_dir / "not_data.dvc").exists()

    assert not (tmp_dir / "data").exists()
    assert not (tmp_dir / "data.dvc").exists()

    shutil.rmtree(not_data)
    dvc.checkout(str(not_data))
    assert not_data.read_text() == {"foo": "foo", "bar": "bar"}
    assert not (tmp_dir / "data").exists()


def test_add_to_cache_not_exists(tmp_dir, dvc, local_cloud):
    local_cloud.gen({"data": {"foo": "foo", "bar": "bar"}})

    dest_dir = tmp_dir / "dir" / "that" / "does" / "not" / "exist"
    with pytest.raises(StagePathNotFoundError):
        dvc.add(str(local_cloud / "data"), out=str(dest_dir))

    dest_dir.parent.mkdir(parents=True)
    dvc.add(str(local_cloud / "data"), out=str(dest_dir))

    assert dest_dir.read_text() == {"foo": "foo", "bar": "bar"}
    assert dest_dir.with_suffix(".dvc").exists()


def test_add_to_cache_from_remote(tmp_dir, dvc, workspace):
    workspace.gen("foo", "foo")

    url = "remote://workspace/foo"
    dvc.add(url, out="foo")

    foo = tmp_dir / "foo"
    assert foo.read_text() == "foo"
    assert (tmp_dir / "foo.dvc").exists()

    # Change the contents of the remote location, in order to
    # ensure it retrieves file from the cache and not re-fetches it
    (workspace / "foo").write_text("bar")

    foo.unlink()
    dvc.checkout(str(foo))
    assert foo.read_text() == "foo"


def test_add_ignored(tmp_dir, scm, dvc):
    from dvc.dvcfile import FileIsGitIgnored

    tmp_dir.gen({"dir": {"subdir": {"file": "content"}}, ".gitignore": "dir/"})
    with pytest.raises(FileIsGitIgnored) as exc:
        dvc.add(targets=[os.path.join("dir", "subdir")])
    assert str(exc.value) == ("bad DVC file name '{}' is git-ignored.").format(
        os.path.join("dir", "subdir.dvc")
    )


def test_add_on_not_existing_file_should_not_remove_stage_file(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    (tmp_dir / "foo").unlink()
    dvcfile_contents = (tmp_dir / stage.path).read_text()

    with pytest.raises(OutputDoesNotExistError):
        dvc.add("foo")
    assert (tmp_dir / "foo.dvc").exists()
    assert (tmp_dir / stage.path).read_text() == dvcfile_contents


@pytest.mark.parametrize(
    "target",
    [
        "dvc.repo.index.Index.check_graph",
        "dvc.stage.Stage.add_outs",
    ],
)
def test_add_does_not_remove_stage_file_on_failure(tmp_dir, dvc, mocker, target):
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    tmp_dir.gen("foo", "foobar")  # update file
    dvcfile_contents = (tmp_dir / stage.path).read_text()

    exc_msg = f"raising error from mocked '{target}'"
    mocker.patch(target, side_effect=DvcException(exc_msg))

    with pytest.raises(DvcException, match=exc_msg):
        dvc.add("foo")
    assert (tmp_dir / "foo.dvc").exists()
    assert (tmp_dir / stage.path).read_text() == dvcfile_contents


def test_add_updates_to_cloud_versioning_dir(tmp_dir, dvc):
    data_dvc = tmp_dir / "data.dvc"
    data_dvc.dump(
        {
            "outs": [
                {
                    "path": "data",
                    "hash": "md5",
                    "files": [
                        {
                            "size": 3,
                            "version_id": "WYRG4BglP7pD.gEoJP6a4AqOhl.FRA.h",
                            "etag": "acbd18db4cc2f85cedef654fccc4a4d8",
                            "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                            "relpath": "bar",
                        },
                        {
                            "size": 3,
                            "version_id": "0vL53tFVY5vVAoJ4HG2jCS1mEcohDPE0",
                            "etag": "acbd18db4cc2f85cedef654fccc4a4d8",
                            "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                            "relpath": "foo",
                        },
                    ],
                }
            ]
        }
    )

    data = tmp_dir / "data"
    data.mkdir()
    (data / "foo").write_text("foo")
    (data / "bar").write_text("bar2")

    dvc.add("data")

    assert (tmp_dir / "data.dvc").parse() == {
        "outs": [
            {
                "path": "data",
                "hash": "md5",
                "files": [
                    {
                        "size": 4,
                        "md5": "224e2539f52203eb33728acd228b4432",
                        "relpath": "bar",
                    },
                    {
                        "size": 3,
                        "version_id": "0vL53tFVY5vVAoJ4HG2jCS1mEcohDPE0",
                        "etag": "acbd18db4cc2f85cedef654fccc4a4d8",
                        "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                        "relpath": "foo",
                    },
                ],
            }
        ]
    }

from os import fspath
from os.path import join

import pytest

from dvc.repo import Repo
from dvc.repo.data import _transform_git_paths_to_dvc, posixpath_to_os_path
from dvc.testing.tmp_dir import make_subrepo
from dvc.utils.fs import remove

EMPTY_STATUS = {
    "committed": {},
    "uncommitted": {},
    "git": {},
    "not_in_cache": [],
    "not_in_remote": [],
    "unchanged": [],
    "untracked": [],
}


@pytest.mark.parametrize("path", [None, ("sub", "repo")])
def test_git_to_dvc_path_wdir_transformation(tmp_dir, scm, path):
    struct = {"dir": {"foo": "foo", "bar": "bar"}, "file": "file", "dir2": {}}
    tmp_dir.gen(struct)

    subdir = tmp_dir.joinpath(*path) if path else tmp_dir
    make_subrepo(subdir, scm)
    dvc = subdir.dvc

    with subdir.chdir():
        subdir.gen(struct)
        _, _, untracked = scm.status(untracked_files="all")
        # make order independent of the platforms for easier test assertions
        untracked = sorted(map(posixpath_to_os_path, untracked), reverse=True)
        assert _transform_git_paths_to_dvc(dvc, untracked) == [
            "file",
            join("dir", "foo"),
            join("dir", "bar"),
        ]
        with (subdir / "dir").chdir():
            assert _transform_git_paths_to_dvc(dvc, untracked) == [
                join("..", "file"),
                "foo",
                "bar",
            ]
        with (subdir / "dir2").chdir():
            assert _transform_git_paths_to_dvc(dvc, untracked) == [
                join("..", "file"),
                join("..", "dir", "foo"),
                join("..", "dir", "bar"),
            ]


def test_file(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen("foo", "foo", commit="add foo")
    tmp_dir.dvc_gen("foo", "foobar")
    remove(tmp_dir / "foo")

    expected = {
        **EMPTY_STATUS,
        "committed": {"modified": ["foo"]},
        "uncommitted": {"deleted": ["foo"]},
        "git": M.dict(),
    }
    assert dvc.data_status() == expected
    assert dvc.data_status(granular=True) == expected


def test_directory(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"dir": {"foo": "foo"}}, commit="add dir")
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar", "foobar": "foobar"}})
    remove(tmp_dir / "dir")
    (tmp_dir / "dir").gen({"foo": "foo", "bar": "barr", "baz": "baz"})
    tmp_dir.gen("untracked", "untracked")

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "committed": {"modified": [join("dir", "")]},
        "uncommitted": {"modified": [join("dir", "")]},
        "git": M.dict(),
    }

    assert dvc.data_status(granular=True, untracked_files="all") == {
        **EMPTY_STATUS,
        "committed": {
            "added": M.unordered(
                join("dir", "bar"),
                join("dir", "foobar"),
            ),
            "modified": [join("dir", "")],
        },
        "uncommitted": {
            "added": [join("dir", "baz")],
            "modified": M.unordered(join("dir", ""), join("dir", "bar")),
            "deleted": [join("dir", "foobar")],
        },
        "git": M.dict(),
        "not_in_cache": [],
        "unchanged": [join("dir", "foo")],
        "untracked": ["untracked"],
    }


def test_tracked_directory_deep(M, tmp_dir, dvc, scm):
    """Test for a directory not in cwd, but nested inside other directories."""
    (tmp_dir / "sub").gen({"dir": {"foo": "foo"}})
    dvc.add(fspath(tmp_dir / "sub" / "dir"))
    scm.add_commit(["sub/dir.dvc", "sub/.gitignore"], message="add sub/dir")

    (tmp_dir / "sub" / "dir").gen("bar", "bar")
    dvc.commit(None, force=True)
    (tmp_dir / "sub" / "dir").gen("foobar", "foobar")

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "committed": {"modified": [join("sub", "dir", "")]},
        "uncommitted": {"modified": [join("sub", "dir", "")]},
        "git": M.dict(),
    }
    assert dvc.data_status(granular=True, untracked_files="all") == {
        **EMPTY_STATUS,
        "committed": {
            "added": [join("sub", "dir", "bar")],
            "modified": [join("sub", "dir", "")],
        },
        "uncommitted": {
            "added": [join("sub", "dir", "foobar")],
            "modified": [join("sub", "dir", "")],
        },
        "git": M.dict(),
        "unchanged": [join("sub", "dir", "foo")],
    }


def test_new_empty_git_repo(M, tmp_dir, scm):
    dvc = Repo.init()
    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "git": M.dict(
            is_empty=True,
            is_dirty=True,
        ),
    }


def test_noscm_repo(dvc):
    assert dvc.data_status() == EMPTY_STATUS


def test_unchanged(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"dir": {"foo": "foo"}}, commit="add dir")
    tmp_dir.dvc_gen("bar", "bar", commit="add foo")

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "git": M.dict(),
        "unchanged": M.unordered("bar", join("dir", "")),
    }
    assert dvc.data_status(granular=True) == {
        **EMPTY_STATUS,
        "git": M.dict(),
        "unchanged": M.unordered("bar", join("dir", ""), join("dir", "foo")),
    }


def test_skip_uncached_pipeline_outputs(tmp_dir, dvc, run_copy_metrics):
    tmp_dir.gen({"m_temp.yaml": str(5)})
    run_copy_metrics(
        "m_temp.yaml",
        "m.yaml",
        metrics_no_cache=["m.yaml"],
        name="copy-metrics",
    )
    assert dvc.data_status() == EMPTY_STATUS
    assert dvc.data_status(granular=True, untracked_files="all") == EMPTY_STATUS


def test_outs_with_no_hashes(M, tmp_dir, dvc, scm):
    dvc.stage.add(single_stage=True, outs=["bar"])
    dvc.stage.add(deps=["bar"], outs=["foo"], name="copy", cmd="cp foo bar")

    expected_output = {
        **EMPTY_STATUS,
        "git": M.dict(),
    }
    assert dvc.data_status() == expected_output
    assert dvc.data_status(granular=True) == expected_output


def test_outs_with_no_hashes_and_with_uncommitted_files(M, tmp_dir, dvc, scm):
    tmp_dir.gen({"bar": "bar", "foo": "foo"})
    dvc.stage.add(single_stage=True, outs=["bar"])
    dvc.stage.add(deps=["bar"], outs=["foo"], name="copy", cmd="cp foo bar")

    expected_output = {
        **EMPTY_STATUS,
        "uncommitted": {"added": M.unordered("bar", "foo")},
        "git": M.dict(),
    }
    assert dvc.data_status() == expected_output
    assert dvc.data_status(granular=True) == expected_output


def test_subdir(M, tmp_dir, scm):
    subrepo = tmp_dir / "sub"
    make_subrepo(subrepo, scm)

    with subrepo.chdir():
        subrepo.dvc_gen({"dir": {"foo": "foo"}}, commit="add dir")
        subrepo.dvc_gen("bar", "bar", commit="add foo")
        subrepo.gen("untracked", "untracked")

        dvc = subrepo.dvc
        assert dvc.data_status(granular=True, untracked_files="all") == {
            **EMPTY_STATUS,
            "git": M.dict(),
            "unchanged": M.unordered("bar", join("dir", ""), join("dir", "foo")),
            "untracked": ["untracked"],
        }


def test_untracked_newly_added_files(M, tmp_dir, dvc, scm):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    tmp_dir.gen("foobar", "foobar")

    expected = {
        **EMPTY_STATUS,
        "untracked": M.unordered(join("dir", "foo"), join("dir", "bar"), "foobar"),
        "git": M.dict(),
    }
    assert dvc.data_status(untracked_files="all") == expected
    assert dvc.data_status(granular=True, untracked_files="all") == expected


def test_missing_cache_workspace_exists(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    tmp_dir.dvc_gen("foobar", "foobar")
    remove(dvc.cache.repo.path)

    assert dvc.data_status(untracked_files="all") == {
        **EMPTY_STATUS,
        "untracked": M.unordered("foobar.dvc", "dir.dvc", ".gitignore"),
        "committed": {"added": M.unordered("foobar", join("dir", ""))},
        "not_in_cache": M.unordered("foobar", join("dir", "")),
        "git": M.dict(),
    }

    assert dvc.data_status(granular=True, untracked_files="all") == {
        **EMPTY_STATUS,
        "untracked": M.unordered("foobar.dvc", "dir.dvc", ".gitignore"),
        "committed": {"added": M.unordered("foobar", join("dir", ""))},
        "uncommitted": {"unknown": M.unordered(join("dir", "foo"), join("dir", "bar"))},
        "not_in_cache": M.unordered(
            "foobar",
            join("dir", ""),
        ),
        "git": M.dict(),
    }


def test_missing_cache_missing_workspace(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    tmp_dir.dvc_gen("foobar", "foobar")
    for path in [dvc.cache.repo.path, "dir", "foobar"]:
        remove(path)

    assert dvc.data_status(untracked_files="all") == {
        **EMPTY_STATUS,
        "untracked": M.unordered("foobar.dvc", "dir.dvc", ".gitignore"),
        "uncommitted": {"deleted": M.unordered("foobar", join("dir", ""))},
        "committed": {"added": M.unordered("foobar", join("dir", ""))},
        "not_in_cache": M.unordered("foobar", join("dir", "")),
        "git": M.dict(),
    }

    assert dvc.data_status(granular=True, untracked_files="all") == {
        **EMPTY_STATUS,
        "untracked": M.unordered("foobar.dvc", "dir.dvc", ".gitignore"),
        "uncommitted": {"deleted": M.unordered("foobar", join("dir", ""))},
        "committed": {"added": M.unordered("foobar", join("dir", ""))},
        "not_in_cache": M.unordered("foobar", join("dir", "")),
        "git": M.dict(),
    }


def test_git_committed_missing_cache_workspace_exists(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}}, commit="add dir")
    tmp_dir.dvc_gen("foobar", "foobar", commit="add foobar")
    remove(dvc.cache.local.path)

    assert dvc.data_status(untracked_files="all") == {
        **EMPTY_STATUS,
        "not_in_cache": M.unordered("foobar", join("dir", "")),
        "git": M.dict(),
        "unchanged": M.unordered("foobar", join("dir", "")),
    }
    assert dvc.data_status(granular=True) == {
        **EMPTY_STATUS,
        "not_in_cache": M.unordered(
            "foobar",
            join("dir", ""),
        ),
        "uncommitted": {"unknown": M.unordered(join("dir", "foo"), join("dir", "bar"))},
        "git": M.dict(),
        "unchanged": M.unordered("foobar", join("dir", "")),
    }


def test_git_committed_missing_cache_missing_workspace(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}}, commit="add dir")
    tmp_dir.dvc_gen("foobar", "foobar", commit="add foobar")
    for path in [dvc.cache.repo.path, "dir", "foobar"]:
        remove(path)

    assert dvc.data_status(untracked_files="all") == {
        **EMPTY_STATUS,
        "uncommitted": {"deleted": M.unordered(join("dir", ""), "foobar")},
        "not_in_cache": M.unordered(join("dir", ""), "foobar"),
        "git": M.dict(),
    }
    assert dvc.data_status(granular=True, untracked_files="all") == {
        **EMPTY_STATUS,
        "committed": {},
        "uncommitted": {"deleted": M.unordered(join("dir", ""), "foobar")},
        "not_in_cache": M.unordered(join("dir", ""), "foobar"),
        "git": M.dict(),
    }


def test_partial_missing_cache(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})

    # remove "foo" from cache
    odb = dvc.cache.repo
    odb.fs.rm(odb.oid_to_path("acbd18db4cc2f85cedef654fccc4a4d8"))

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "committed": {"added": [join("dir", "")]},
        "git": M.dict(),
    }
    assert dvc.data_status(granular=True) == {
        **EMPTY_STATUS,
        "committed": {
            "added": M.unordered(
                join("dir", ""), join("dir", "foo"), join("dir", "bar")
            )
        },
        "not_in_cache": [join("dir", "foo")],
        "git": M.dict(),
    }


def test_missing_dir_object_from_head(M, tmp_dir, dvc, scm):
    (stage,) = tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}}, commit="add dir")
    remove("dir")
    tmp_dir.dvc_gen({"dir": {"foobar": "foobar"}})
    odb = dvc.cache.repo
    odb.fs.rm(odb.oid_to_path(stage.outs[0].hash_info.value))

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "committed": {"modified": [join("dir", "")]},
        "git": M.dict(),
    }
    assert dvc.data_status(granular=True) == {
        **EMPTY_STATUS,
        "committed": {
            "modified": [join("dir", "")],
            "unknown": [join("dir", "foobar")],
        },
        "git": M.dict(),
    }


def test_missing_dir_object_from_index(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}}, commit="add dir")
    remove("dir")
    (stage,) = tmp_dir.dvc_gen({"dir": {"foobar": "foobar"}})
    odb = dvc.cache.repo
    odb.fs.rm(odb.oid_to_path(stage.outs[0].hash_info.value))

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "committed": {"modified": [join("dir", "")]},
        "not_in_cache": [join("dir", "")],
        "git": M.dict(),
    }
    assert dvc.data_status(granular=True) == {
        **EMPTY_STATUS,
        "committed": {
            "modified": [join("dir", "")],
        },
        "uncommitted": {"unknown": [join("dir", "foobar")]},
        "not_in_cache": [join("dir", "")],
        "git": M.dict(),
    }


def test_missing_remote_cache(M, tmp_dir, dvc, scm, local_remote):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    tmp_dir.dvc_gen("foobar", "foobar")

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "committed": {"added": M.unordered("foobar", join("dir", ""))},
        "git": M.dict(),
    }

    assert dvc.data_status(untracked_files="all", not_in_remote=True) == {
        **EMPTY_STATUS,
        "untracked": M.unordered("foobar.dvc", "dir.dvc", ".gitignore"),
        "committed": {"added": M.unordered("foobar", join("dir", ""))},
        "not_in_remote": M.unordered("foobar", join("dir", "")),
        "git": M.dict(),
    }

    assert dvc.data_status(
        granular=True, untracked_files="all", not_in_remote=True
    ) == {
        **EMPTY_STATUS,
        "untracked": M.unordered("foobar.dvc", "dir.dvc", ".gitignore"),
        "committed": {
            "added": M.unordered(
                "foobar",
                join("dir", ""),
                join("dir", "foo"),
                join("dir", "bar"),
            )
        },
        "uncommitted": {},
        "not_in_remote": M.unordered(
            "foobar",
            join("dir", ""),
            join("dir", "foo"),
            join("dir", "bar"),
        ),
        "git": M.dict(),
    }


def test_root_from_dir_to_file(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"data": {"foo": "foo", "bar": "bar"}})
    remove("data")
    tmp_dir.gen("data", "file")

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "committed": {"added": [join("data", "")]},
        "uncommitted": {"modified": ["data"]},
        "git": M.dict(),
    }
    assert dvc.data_status(granular=True) == {
        **EMPTY_STATUS,
        "committed": {
            "added": M.unordered(
                join("data", ""), join("data", "foo"), join("data", "bar")
            )
        },
        "uncommitted": {
            "deleted": M.unordered(join("data", "foo"), join("data", "bar")),
            "modified": ["data"],
        },
        "git": M.dict(),
    }


def test_root_from_file_to_dir(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen("data", "file")
    remove("data")
    tmp_dir.gen({"data": {"foo": "foo", "bar": "bar"}})

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "committed": {"added": ["data"]},
        "uncommitted": {"modified": [join("data", "")]},
        "git": M.dict(),
    }
    assert dvc.data_status(granular=True) == {
        **EMPTY_STATUS,
        "committed": {"added": ["data"]},
        "uncommitted": {
            "modified": [join("data", "")],
            "added": M.unordered(join("data", "foo"), join("data", "bar")),
        },
        "git": M.dict(),
    }


def test_empty_dir(tmp_dir, scm, dvc, M):
    # regression testing for https://github.com/iterative/dvc/issues/8958
    tmp_dir.dvc_gen({"data": {"foo": "foo"}})
    remove("data")

    (tmp_dir / "data").mkdir()

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "committed": {"added": [join("data", "")]},
        "uncommitted": {"modified": [join("data", "")]},
        "git": M.dict(),
    }

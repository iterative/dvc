from collections.abc import Iterable
from functools import partial
from os import fspath
from os.path import join
from typing import TYPE_CHECKING

import pytest
from pytest_test_utils import matchers as m

from dvc.repo import Repo
from dvc.repo.data import _transform_git_paths_to_dvc, posixpath_to_os_path
from dvc.testing.tmp_dir import TmpDir, make_subrepo
from dvc.utils.fs import remove

if TYPE_CHECKING:
    from dvc.stage import Stage

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
            "added": M.unordered(join("dir", "bar"), join("dir", "foobar")),
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
        "git": M.dict(is_empty=True, is_dirty=True),
    }

    tmp_dir.gen("foo", "foo")
    dvc.add(["foo"])
    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "git": M.dict(is_empty=True, is_dirty=True),
        "committed": {"added": ["foo"]},
    }


def test_noscm_repo(tmp_dir, dvc):
    assert dvc.data_status() == EMPTY_STATUS
    tmp_dir.dvc_gen("foo", "foo")
    assert dvc.data_status() == {**EMPTY_STATUS, "unchanged": ["foo"]}


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

    expected_output = {**EMPTY_STATUS, "git": M.dict()}
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
        "not_in_cache": M.unordered("foobar", join("dir", "")),
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
        "not_in_cache": M.unordered("foobar", join("dir", "")),
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
        "committed": {"modified": [join("dir", "")]},
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


def test_not_in_remote_respects_not_pushable(
    M: type["m.Matcher"], tmp_dir: TmpDir, dvc: Repo, scm, mocker, local_remote
):
    stages: list[Stage] = tmp_dir.dvc_gen({"foo": "foo", "dir": {"foobar": "foobar"}})
    # Make foo not pushable
    stages[0].outs[0].can_push = False
    stages[0].dump()

    def assert_not_in_remote_is(
        granular: bool, not_in_remote: list[str], committed: list[str]
    ):
        assert dvc.data_status(
            granular=granular, remote_refresh=True, not_in_remote=True
        ) == {
            **EMPTY_STATUS,
            "git": M.dict(),
            "not_in_remote": M.unordered(*not_in_remote),
            "committed": {"added": M.unordered(*committed)},
        }

    foo = "foo"
    dir_ = join("dir", "")
    foobar = join("dir", "foobar")

    assert_not_in_remote_is(
        granular=True,
        not_in_remote=[dir_, foobar],
        committed=[foo, dir_, foobar],
    )
    assert_not_in_remote_is(granular=False, not_in_remote=[dir_], committed=[foo, dir_])

    dvc.push()

    assert_not_in_remote_is(
        granular=True,
        not_in_remote=[],
        committed=[foo, dir_, foobar],
    )
    assert_not_in_remote_is(
        granular=False,
        not_in_remote=[],
        committed=[foo, dir_],
    )


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


def test_untracked_files_filter_targets(M, tmp_dir, scm, dvc):
    tmp_dir.gen(
        {"spam": "spam", "ham": "ham", "dir": {"eggs": "eggs", "bacon": "bacon"}}
    )
    _default = EMPTY_STATUS | {"git": M.dict()}
    status = partial(dvc.data_status, untracked_files="all")

    assert status(["not-existing"]) == _default

    assert status(["spam"]) == _default | {"untracked": ["spam"]}
    assert status(["spam", "ham"]) == _default | {
        "untracked": M.unordered("spam", "ham")
    }
    assert status(["dir"]) == _default | {
        "untracked": M.unordered(join("dir", "eggs"), join("dir", "bacon")),
    }
    assert status([join("dir", "")]) == _default | {
        "untracked": M.unordered(join("dir", "eggs"), join("dir", "bacon")),
    }
    assert status([join("dir", "bacon")]) == _default | {
        "untracked": [join("dir", "bacon")]
    }


def param(*values):
    """Uses test id from the first value."""
    first = values[0]
    _id = (
        ",".join(first)
        if isinstance(first, Iterable) and not isinstance(first, str)
        else first
    )
    return pytest.param(*values, id=_id)


@pytest.mark.parametrize(
    "targets,expected",
    [
        param(
            ["foo"],
            {"committed": {"added": ["foo"]}, "uncommitted": {"deleted": ["foo"]}},
        ),
        param(
            ["bar"],
            {"committed": {"added": ["bar"]}, "uncommitted": {"modified": ["bar"]}},
        ),
        param(["foobar"], {"committed": {"added": ["foobar"]}, "uncommitted": {}}),
        param(["not-existing"], {}),
        param(["baz"], {"untracked": ["baz"]}),
        param(
            ["foo", "foobar"],
            {
                "committed": {"added": m.unordered("foo", "foobar")},
                "uncommitted": {"deleted": ["foo"]},
            },
        ),
    ],
)
def test_filter_targets_files_after_dvc_commit(M, tmp_dir, dvc, scm, targets, expected):
    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar", "foobar": "foobar"})
    (tmp_dir / "foo").unlink()  # deleted
    tmp_dir.gen({"bar": "bar modified", "baz": "baz new"})

    assert dvc.data_status(
        targets=targets, untracked_files="all"
    ) == EMPTY_STATUS | expected | {"git": M.dict()}
    assert dvc.data_status(
        targets=targets, granular=True, untracked_files="all"
    ) == EMPTY_STATUS | expected | {"git": M.dict()}


@pytest.mark.parametrize(
    "targets,expected",
    [
        param(["not-existing"], {}),
        param(["foo"], {"uncommitted": {"deleted": ["foo"]}}),
        param(["bar"], {"unchanged": ["bar"]}),
        param(["baz"], {"unchanged": ["baz"]}),
        param(["foobar"], {"unchanged": ["foobar"]}),
        param(
            ("foo", "foobar"),
            {"unchanged": ["foobar"], "uncommitted": {"deleted": ["foo"]}},
        ),
    ],
)
def test_filter_targets_after_git_commit(M, tmp_dir, dvc, scm, targets, expected):
    tmp_dir.dvc_gen(
        {"foo": "foo", "bar": "bar", "foobar": "foobar", "baz": "baz"},
        commit="add files",
    )
    (tmp_dir / "foo").unlink()  # deleted

    assert dvc.data_status(
        targets=targets, untracked_files="all"
    ) == EMPTY_STATUS | expected | {"git": M.dict()}
    assert dvc.data_status(
        targets=targets, granular=True, untracked_files="all"
    ) == EMPTY_STATUS | expected | {"git": M.dict()}


def with_aliases(values, aliases):
    """Generate test cases by reusing values for given aliases from existing ones."""
    for value in values:
        targets = value[0]
        assert isinstance(targets, tuple)
        yield param(*value)
    yield from (
        param(alias, *rest)
        for alias, to in aliases.items()
        for targets, *rest in values
        if to == targets
    )


@pytest.mark.parametrize(
    "targets,expected_ng,expected_g",
    with_aliases(
        [
            (
                ("dir",),
                {
                    "committed": {"added": [join("dir", "")]},
                    "uncommitted": {"modified": [join("dir", "")]},
                },
                {
                    "committed": {
                        "added": m.unordered(
                            join("dir", ""),
                            join("dir", "foo"),
                            join("dir", "sub", "bar"),
                            join("dir", "foobar"),
                        )
                    },
                    "uncommitted": {
                        "added": [join("dir", "baz")],
                        "modified": [join("dir", ""), join("dir", "sub", "bar")],
                        "deleted": [join("dir", "foo")],
                    },
                },
            ),
            (
                (join("dir", "foo"),),
                {},
                {
                    "committed": {"added": [join("dir", "foo")]},
                    "uncommitted": {"deleted": [join("dir", "foo")]},
                },
            ),
            (
                (join("dir", "baz"),),
                {},
                {
                    "uncommitted": {"added": [join("dir", "baz")]},
                },
            ),
            (
                (join("dir", "sub"),),
                {},
                {
                    "committed": {"added": [join("dir", "sub", "bar")]},
                    "uncommitted": {"modified": [join("dir", "sub", "bar")]},
                },
            ),
            (
                (join("dir", "sub", "bar"),),
                {},
                {
                    "committed": {"added": [join("dir", "sub", "bar")]},
                    "uncommitted": {"modified": [join("dir", "sub", "bar")]},
                },
            ),
            (
                (join("dir", "foobar"),),
                {},
                {
                    "committed": {"added": [join("dir", "foobar")]},
                    "uncommitted": {},
                },
            ),
            ((join("dir", "not-existing-file"),), {}, {}),
            ((join("dir", "not-existing-dir", ""),), {}, {}),
            ((join("dir", "sub", "not-existing-file"),), {}, {}),
            (
                (join("dir", "foo"), join("dir", "foobar")),
                {},
                {
                    "committed": {
                        "added": m.unordered(join("dir", "foo"), join("dir", "foobar"))
                    },
                    "uncommitted": {"deleted": [join("dir", "foo")]},
                },
            ),
        ],
        {
            # the values for these are used from above test cases
            (join("dir", ""),): ("dir",),
            (join("dir", "sub", ""),): (join("dir", "sub"),),
            (join("dir", ""), join("dir", "foo")): ("dir",),
        },
    ),
)
def test_filter_targets_inside_directory_after_dvc_commit(
    M, tmp_dir, dvc, scm, targets, expected_ng, expected_g
):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "sub": {"bar": "bar"}, "foobar": "foobar"}})
    (tmp_dir / "dir" / "foo").unlink()  # deleted
    (tmp_dir / "dir" / "sub" / "bar").write_text("bar modified")
    (tmp_dir / "dir" / "baz").write_text("baz new")

    assert dvc.data_status(
        targets=targets, untracked_files="all"
    ) == EMPTY_STATUS | expected_ng | {"git": M.dict()}
    assert dvc.data_status(
        targets=targets, granular=True, untracked_files="all"
    ) == EMPTY_STATUS | expected_g | {"git": M.dict()}


@pytest.mark.parametrize(
    "targets,expected_ng,expected_g",
    with_aliases(
        [
            (
                (join("dir", "foo"),),
                {},
                {"committed": {"deleted": [join("dir", "foo")]}},
            ),
            ((join("dir", "baz"),), {}, {"committed": {"added": [join("dir", "baz")]}}),
            ((join("dir", "foobar"),), {}, {"unchanged": [join("dir", "foobar")]}),
            (
                ("dir",),
                {"committed": {"modified": [join("dir", "")]}},
                {
                    "unchanged": [join("dir", "foobar")],
                    "committed": {
                        "added": [join("dir", "baz")],
                        "modified": m.unordered(
                            join("dir", ""), join("dir", "sub", "bar")
                        ),
                        "deleted": [join("dir", "foo")],
                    },
                },
            ),
            (
                (join("dir", "sub"),),
                {},
                {"committed": {"modified": [join("dir", "sub", "bar")]}},
            ),
            (
                (join("dir", "sub", "bar"),),
                {},
                {"committed": {"modified": [join("dir", "sub", "bar")]}},
            ),
            (
                (join("dir", "foo"), join("dir", "foobar")),
                {},
                {
                    "unchanged": [join("dir", "foobar")],
                    "committed": {"deleted": [join("dir", "foo")]},
                },
            ),
        ],
        {
            (join("dir", ""),): ("dir",),
            (join("dir", "sub", ""),): (join("dir", "sub"),),
            (join("dir", ""), join("dir", "foo")): ("dir",),
        },
    ),
)
def test_filter_targets_inside_directory_after_git_commit(
    M, tmp_dir, dvc, scm, targets, expected_ng, expected_g
):
    tmp_dir.dvc_gen(
        {"dir": {"foo": "foo", "sub": {"bar": "bar"}, "foobar": "foobar"}},
        commit="add dir",
    )
    (tmp_dir / "dir" / "foo").unlink()  # deleted
    (tmp_dir / "dir" / "sub" / "bar").write_text("bar modified")
    (tmp_dir / "dir" / "baz").write_text("baz new")
    dvc.add(["dir"])

    assert dvc.data_status(
        targets=targets, untracked_files="all"
    ) == EMPTY_STATUS | expected_ng | {"git": M.dict()}
    assert dvc.data_status(
        targets=targets, granular=True, untracked_files="all"
    ) == EMPTY_STATUS | expected_g | {"git": M.dict()}


@pytest.mark.parametrize("to_check", ["remote", "cache"])
@pytest.mark.parametrize(
    "targets, non_granular, granular",
    [
        param(("foo",), ["foo"], ["foo"]),
        param(("dir",), [join("dir", "")], [join("dir", ""), join("dir", "bar")]),
        param(
            (join("dir", ""),), [join("dir", "")], [join("dir", ""), join("dir", "bar")]
        ),
        param((join("dir", "bar"),), [], [join("dir", "bar")]),
        param(
            (join("dir", "bar"), "foo"),
            ["foo"],
            m.unordered(join("dir", "bar"), "foo"),
        ),
        param(
            (join("dir", "bar"), "dir"),
            [join("dir", "")],
            m.unordered(join("dir", ""), join("dir", "bar")),
        ),
        param(
            ("dir", "foo"),
            m.unordered(join("dir", ""), "foo"),
            m.unordered(join("dir", ""), join("dir", "bar"), "foo"),
        ),
    ],
)
def test_filter_targets_not_in_cache(
    M, local_remote, tmp_dir, scm, dvc, to_check, targets, non_granular, granular
):
    tmp_dir.dvc_gen({"foo": "foo", "dir": {"bar": "bar"}})

    if to_check == "cache":
        dvc.push()
        dvc.cache.local.clear()

    not_in_remote = to_check == "remote"
    key = "not_in_" + to_check
    d = EMPTY_STATUS | {"git": M.dict(), "committed": M.dict()}
    assert dvc.data_status(targets, not_in_remote=not_in_remote) == d | {
        key: non_granular
    }
    assert dvc.data_status(targets, granular=True, not_in_remote=not_in_remote) == d | {
        key: granular
    }


def test_compat_legacy_new_cache_types(M, tmp_dir, dvc, scm):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    (tmp_dir / "foo.dvc").dump(
        {
            "outs": [
                {"path": "foo", "md5": "acbd18db4cc2f85cedef654fccc4a4d8", "size": 3},
            ]
        }
    )
    dvc.add(tmp_dir / "bar", no_commit=True)

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "not_in_cache": M.unordered("foo", "bar"),
        "committed": {"added": M.unordered("foo", "bar")},
        "git": M.dict(),
    }

    dvc.commit("foo")

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "not_in_cache": ["bar"],
        "committed": {"added": M.unordered("foo", "bar")},
        "git": M.dict(),
    }

    dvc.commit("bar")

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "not_in_cache": [],
        "committed": {"added": M.unordered("foo", "bar")},
        "git": M.dict(),
    }


def test_missing_cache_remote_check(M, tmp_dir, dvc, scm, local_remote):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    tmp_dir.dvc_gen("foobar", "foobar")
    remove(dvc.cache.repo.path)

    assert dvc.data_status(untracked_files="all", not_in_remote=True) == {
        **EMPTY_STATUS,
        "untracked": M.unordered("foobar.dvc", "dir.dvc", ".gitignore"),
        "committed": {"added": M.unordered("foobar", join("dir", ""))},
        "not_in_cache": M.unordered("foobar", join("dir", "")),
        "git": M.dict(),
        "not_in_remote": M.unordered("foobar", join("dir", "")),
    }

    assert dvc.data_status(
        granular=True, untracked_files="all", not_in_remote=True
    ) == {
        **EMPTY_STATUS,
        "untracked": M.unordered("foobar.dvc", "dir.dvc", ".gitignore"),
        "committed": {"added": M.unordered("foobar", join("dir", ""))},
        "uncommitted": {"unknown": M.unordered(join("dir", "foo"), join("dir", "bar"))},
        "not_in_cache": M.unordered("foobar", join("dir", "")),
        "git": M.dict(),
        "not_in_remote": M.unordered("foobar", join("dir", "")),
    }

    assert dvc.data_status(["dir"], untracked_files="all", not_in_remote=True) == {
        **EMPTY_STATUS,
        "committed": {"added": [join("dir", "")]},
        "not_in_cache": [join("dir", "")],
        "git": M.dict(),
        "not_in_remote": [join("dir", "")],
    }

    assert dvc.data_status(
        ["dir"], untracked_files="all", not_in_remote=True, granular=True
    ) == {
        **EMPTY_STATUS,
        "committed": {"added": [join("dir", "")]},
        "uncommitted": {"unknown": M.unordered(join("dir", "foo"), join("dir", "bar"))},
        "not_in_cache": [join("dir", "")],
        "git": M.dict(),
        "not_in_remote": [join("dir", "")],
    }

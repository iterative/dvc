from os.path import join

from dvc.repo import Repo
from dvc.testing.tmp_dir import make_subrepo
from dvc.utils.fs import remove

EMPTY_STATUS = {
    "committed": {},
    "uncommitted": {},
    "git": {},
    "not_in_cache": [],
    "unchanged": [],
    "untracked": [],
}


def test_file(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen("foo", "foo", commit="add foo")
    tmp_dir.dvc_gen("foo", "foobar")
    remove(tmp_dir / "foo")

    expected = {
        "committed": {"modified": ["foo"]},
        "uncommitted": {"deleted": ["foo"]},
        "git": M.dict(),
        "not_in_cache": [],
        "unchanged": [],
        "untracked": [],
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
        "committed": {"modified": [join("dir", "")]},
        "uncommitted": {"modified": [join("dir", "")]},
        "git": M.dict(),
        "not_in_cache": [],
        "unchanged": [],
        "untracked": [],
    }

    assert dvc.data_status(granular=True, untracked_files="all") == {
        "committed": {
            "added": M.unordered(
                join("dir", "bar"),
                join("dir", "foobar"),
            )
        },
        "uncommitted": {
            "added": [join("dir", "baz")],
            "modified": [join("dir", "bar")],
            "deleted": [join("dir", "foobar")],
        },
        "git": M.dict(),
        "not_in_cache": [],
        "unchanged": [join("dir", "foo")],
        "untracked": ["untracked"],
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
        "unchanged": M.unordered("bar", join("dir", "foo")),
    }


def test_withdirs(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"dir": {"foo": "foo"}}, commit="add dir")
    tmp_dir.dvc_gen("bar", "bar", commit="add foo")
    assert dvc.data_status(granular=True, with_dirs=True) == {
        **EMPTY_STATUS,
        "git": M.dict(),
        "unchanged": M.unordered("bar", join("dir", "foo"), join("dir", "")),
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
    assert (
        dvc.data_status(granular=True, untracked_files="all") == EMPTY_STATUS
    )


def test_output_with_newly_added_stage(tmp_dir, dvc):
    dvc.stage.add(deps=["bar"], outs=["foo"], name="copy", cmd="cp foo bar")
    # assert dvc.data_status() == {**EMPTY_STATUS, "not_in_cache": ["foo"]}
    # TODO: discuss what the output should be
    assert dvc.data_status() == EMPTY_STATUS
    assert dvc.data_status(granular=True) == EMPTY_STATUS


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
            "unchanged": M.unordered("bar", join("dir", "foo")),
            "untracked": ["untracked"],
        }


def test_untracked_newly_added_files(M, tmp_dir, dvc, scm):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    tmp_dir.gen("foobar", "foobar")

    expected = {
        **EMPTY_STATUS,
        "untracked": M.unordered(
            join("dir", "foo"), join("dir", "bar"), "foobar"
        ),
        "git": M.dict(),
    }
    assert dvc.data_status(untracked_files="all") == expected
    assert dvc.data_status(granular=True, untracked_files="all") == expected


def test_missing_cache_workspace_exists(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    tmp_dir.dvc_gen("foobar", "foobar")
    remove(dvc.odb.repo.path)

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
        # "committed": {
        #   "added": M.unordered(
        #       "foobar", join("dir", "foo"), join("dir", "bar")
        # )},
        "committed": {"added": M.unordered("foobar", join("dir", ""))},
        "uncommitted": {
            "unknown": M.unordered(join("dir", "foo"), join("dir", "bar"))
        },
        # even though we don't have cache obj, we can repurpose
        # what we have in the workspace
        "not_in_cache": M.unordered(
            "foobar",
            join("dir", ""),
        ),
        "git": M.dict(),
    }


def test_missing_cache_missing_workspace(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    tmp_dir.dvc_gen("foobar", "foobar")
    for path in [dvc.odb.repo.path, "dir", "foobar"]:
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
    remove(dvc.odb.local.path)

    assert dvc.data_status(untracked_files="all") == {
        **EMPTY_STATUS,
        "not_in_cache": M.unordered("foobar", join("dir", "")),
        "git": M.dict(),
        # TODO: clarify what is unchanged
        "unchanged": M.unordered("foobar", join("dir", "")),
    }
    assert dvc.data_status(granular=True) == {
        **EMPTY_STATUS,
        "not_in_cache": M.unordered(
            "foobar",
            join("dir", ""),
        ),
        "uncommitted": {
            "unknown": M.unordered(join("dir", "foo"), join("dir", "bar"))
        },
        "git": M.dict(),
        # TODO: clarify what is unchanged
        "unchanged": M.unordered("foobar", join("dir", "")),
    }


def test_git_committed_missing_cache_missing_workspace(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}}, commit="add dir")
    tmp_dir.dvc_gen("foobar", "foobar", commit="add foobar")
    for path in [dvc.odb.repo.path, "dir", "foobar"]:
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
    odb = dvc.odb.repo
    odb.fs.rm(odb.oid_to_path("acbd18db4cc2f85cedef654fccc4a4d8"))

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "committed": {"added": [join("dir", "")]},
        "git": M.dict(),
    }
    assert dvc.data_status(granular=True) == {
        **EMPTY_STATUS,
        "committed": {
            "added": M.unordered(join("dir", "foo"), join("dir", "bar"))
        },
        "not_in_cache": [join("dir", "foo")],
        "git": M.dict(),
    }


def test_missing_dir_object_from_head(M, tmp_dir, dvc, scm):
    (stage,) = tmp_dir.dvc_gen(
        {"dir": {"foo": "foo", "bar": "bar"}}, commit="add dir"
    )
    remove("dir")
    tmp_dir.dvc_gen({"dir": {"foobar": "foobar"}})
    odb = dvc.odb.repo
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


def test_root_from_dir_to_file(M, tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"data": {"foo": "foo", "bar": "bar"}})
    remove("data")
    tmp_dir.gen("data", "file")

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "committed": {"added": [join("data", "")]},
        "uncommitted": {"modified": [join("data")]},
        "git": M.dict(),
    }
    assert dvc.data_status(granular=True) == {
        **EMPTY_STATUS,
        "committed": {
            "added": M.unordered(join("data", "foo"), join("data", "bar"))
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
        "committed": {"added": [join("data")]},
        "uncommitted": {
            "modified": [join("data", "")],
            "added": M.unordered(join("data", "foo"), join("data", "bar")),
        },
        "git": M.dict(),
    }

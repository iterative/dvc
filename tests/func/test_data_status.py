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


def test_file(M, tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("foo", "foo", commit="add foo")
    tmp_dir.dvc_gen("foo", "foobar")
    remove(tmp_dir / "foo")

    expected = {
        "committed": {"modified": ["foo"]},
        "uncommitted": {"deleted": ["foo"]},
        "git": M.instance_of(dict),
        "not_in_cache": [],
        "unchanged": [],
        "untracked": [],
    }
    assert dvc.data_status() == expected
    assert dvc.data_status(granular=True) == expected


def test_directory(M, tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"dir": {"foo": "foo"}}, commit="add dir")
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar", "foobar": "foobar"}})
    remove(tmp_dir / "dir")
    (tmp_dir / "dir").gen({"foo": "foo", "bar": "barr", "baz": "baz"})
    tmp_dir.gen("untracked", "untracked")

    assert dvc.data_status() == {
        "committed": {"modified": [join("dir", "")]},
        "uncommitted": {"modified": [join("dir", "")]},
        "git": M.instance_of(dict),
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
        "git": M.instance_of(dict),
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


def test_unchanged(M, tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"dir": {"foo": "foo"}}, commit="add dir")
    tmp_dir.dvc_gen("bar", "bar", commit="add foo")

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "git": M.instance_of(dict),
        "unchanged": M.unordered("bar", join("dir", "")),
    }
    assert dvc.data_status(granular=True) == {
        **EMPTY_STATUS,
        "git": M.instance_of(dict),
        "unchanged": M.unordered("bar", join("dir", "foo")),
    }


def test_not_in_cache(M, tmp_dir, scm, dvc):
    # TODO: investigation required, might return wrong results
    tmp_dir.dvc_gen({"dir": {"foo": "foo"}}, commit="add dir")
    tmp_dir.dvc_gen("bar", "bar", commit="add foo")
    remove(dvc.odb.local.path)

    assert dvc.data_status() == {
        **EMPTY_STATUS,
        "not_in_cache": M.unordered("bar", join("dir", "")),
        "git": M.instance_of(dict),
        "unchanged": ["bar"],
        "uncommitted": {"added": [join("dir", "")]},
    }
    assert dvc.data_status(granular=True) == {
        **EMPTY_STATUS,
        "not_in_cache": M.unordered("bar"),
        "git": M.instance_of(dict),
        "unchanged": [],
        "committed": {"modified": ["bar"]},
        "uncommitted": {"modified": ["bar"], "added": [join("dir", "foo")]},
    }


def test_withdirs(M, tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"dir": {"foo": "foo"}}, commit="add dir")
    tmp_dir.dvc_gen("bar", "bar", commit="add foo")
    assert dvc.data_status(granular=True, with_dirs=True) == {
        **EMPTY_STATUS,
        "git": M.instance_of(dict),
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
    assert dvc.data_status() == {**EMPTY_STATUS, "not_in_cache": ["foo"]}


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
            "git": M.instance_of(dict),
            "unchanged": M.unordered("bar", join("dir", "foo")),
            "untracked": ["untracked"],
        }

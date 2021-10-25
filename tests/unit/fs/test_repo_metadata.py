import os

import pytest

from dvc.fs.repo import RepoFileSystem
from tests.unit.fs.test_repo import make_subrepo


@pytest.fixture
def repo_fs(tmp_dir, dvc, scm):
    fs_structure = {
        "models": {  # mixed dvc + git directory
            "train.py": "train dot py",
            "test.py": "test dot py",
        },
        "README.md": "my little project",  # file
        "src": {  # repo-only directory
            "utils": {
                "__init__.py": "",
                "serve_model.py": "# this will serve a model `soon`",
            }
        },
    }
    dvc_structure = {
        "data": {  # dvc only directory
            "raw": {
                "raw-1.csv": "one, dot, csv",
                "raw-2.csv": "two, dot, csv",
            },
            "processed": {
                "processed-1.csv": "1, dot, csv",
                "processed-2.csv": "2, dot, csv",
            },
        },
        os.path.join("models", "transform.pickle"): "model model",  # file
    }

    tmp_dir.scm_gen(fs_structure, commit="repo init")
    tmp_dir.dvc_gen(dvc_structure, commit="use dvc")

    yield RepoFileSystem(dvc, subrepos=True)


def test_metadata_not_existing(repo_fs):
    path = os.path.join("path", "that", "does", "not", "exist")

    with pytest.raises(FileNotFoundError):
        repo_fs.metadata(path)


@pytest.mark.parametrize(
    "path",
    [
        "README.md",
        os.path.join("models", "train.py"),
        os.path.join("models", "test.py"),
        os.path.join("src", "utils", "__init__.py"),
        os.path.join("src", "utils", "serve_model.py"),
    ],
)
def test_metadata_git_tracked_file(repo_fs, path):
    meta = repo_fs.metadata(path)

    assert meta.fs_path == os.path.join(repo_fs.root_dir, path)
    assert meta.repo.root_dir == repo_fs.root_dir
    assert not meta.is_output
    assert not meta.part_of_output
    assert not meta.contains_outputs
    assert not meta.is_dvc
    assert not meta.output_exists
    assert not meta.isdir
    assert not meta.is_exec
    assert meta.isfile
    assert not meta.outs


@pytest.mark.parametrize(
    "path, outs, is_output",
    [
        (os.path.join("data", "raw", "raw-1.csv"), ["data"], False),
        (os.path.join("data", "raw", "raw-2.csv"), ["data"], False),
        (
            os.path.join("data", "processed", "processed-1.csv"),
            ["data"],
            False,
        ),
        (
            os.path.join("data", "processed", "processed-2.csv"),
            ["data"],
            False,
        ),
        (
            os.path.join("models", "transform.pickle"),
            [os.path.join("models", "transform.pickle")],
            True,
        ),
    ],
)
def test_metadata_dvc_tracked_file(repo_fs, path, outs, is_output):
    meta = repo_fs.metadata(path)

    assert meta.fs_path == os.path.join(repo_fs.root_dir, path)
    assert meta.repo.root_dir == repo_fs.root_dir
    assert meta.is_output == is_output
    assert meta.part_of_output != is_output
    assert not meta.contains_outputs
    assert meta.is_dvc
    assert meta.output_exists
    assert not meta.isdir
    assert not meta.is_exec
    assert meta.isfile
    assert {out.fs_path for out in meta.outs} == {
        os.path.join(repo_fs.root_dir, out) for out in outs
    }


@pytest.mark.parametrize("path", ["src", os.path.join("src", "utils")])
def test_metadata_git_only_dirs(repo_fs, path):
    meta = repo_fs.metadata(path)

    assert meta.fs_path == os.path.join(repo_fs.root_dir, path)
    assert meta.repo.root_dir == repo_fs.root_dir
    assert not meta.is_output
    assert not meta.part_of_output
    assert not meta.contains_outputs
    assert not meta.is_dvc
    assert not meta.output_exists
    assert meta.isdir
    assert meta.is_exec
    assert not meta.isfile
    assert not meta.outs


@pytest.mark.parametrize(
    "path, expected_outs",
    [
        (".", ["data", os.path.join("models", "transform.pickle")]),
        ("models", [os.path.join("models", "transform.pickle")]),
    ],
)
def test_metadata_git_dvc_mixed_dirs(repo_fs, path, expected_outs):
    meta = repo_fs.metadata(os.path.join(repo_fs.root_dir, path))

    assert meta.fs_path == os.path.normpath(
        os.path.join(repo_fs.root_dir, path)
    )
    assert meta.repo.root_dir == repo_fs.root_dir
    assert not meta.is_output
    assert not meta.part_of_output
    assert meta.contains_outputs
    assert not meta.is_dvc
    assert meta.output_exists
    assert meta.isdir
    assert not meta.is_exec
    assert not meta.isfile

    assert {out.fs_path for out in meta.outs} == {
        os.path.join(repo_fs.root_dir, out) for out in expected_outs
    }


@pytest.mark.parametrize(
    "path, is_output",
    [
        ("data", True),
        (os.path.join("data", "raw"), False),  # is inside output
        (os.path.join("data", "processed"), False),
    ],
)
def test_metadata_dvc_only_dirs(repo_fs, path, is_output):
    meta = repo_fs.metadata(os.path.join(repo_fs.root_dir, path))

    assert meta.fs_path == os.path.join(repo_fs.root_dir, path)
    assert meta.repo.root_dir == repo_fs.root_dir
    assert meta.is_output == is_output
    assert meta.part_of_output != is_output
    assert not meta.contains_outputs
    assert meta.is_dvc
    assert meta.output_exists
    assert meta.isdir
    assert not meta.is_exec
    assert not meta.isfile
    assert {out.fs_path for out in meta.outs} == {
        os.path.join(repo_fs.root_dir, "data")
    }


def test_metadata_on_subrepos(make_tmp_dir, tmp_dir, dvc, scm, repo_fs):
    subrepo = tmp_dir / "subrepo"
    make_subrepo(subrepo, scm)
    with subrepo.chdir():
        subrepo.scm_gen("foo", "foo", commit="add foo on subrepo")
        subrepo.dvc_gen("foobar", "foobar", commit="add foobar on subrepo")

    for path in [
        "subrepo",
        os.path.join("subrepo", "foo"),
        os.path.join("subrepo", "foobar"),
    ]:
        meta = repo_fs.metadata(tmp_dir / path)
        assert meta.repo.root_dir == str(
            subrepo
        ), f"repo root didn't match for {path}"

    # supports external outputs on top-level DVC repo
    external_dir = make_tmp_dir("external-output")
    external_dir.gen("bar", "bar")
    dvc.add(str(external_dir / "bar"), external=True)
    meta = repo_fs.metadata((external_dir / "bar").fs_path)
    assert meta.repo.root_dir == str(tmp_dir)

import pytest

from dvc.path_info import PathInfo
from dvc.tree.repo import RepoTree
from tests.unit.tree.test_repo import make_subrepo


@pytest.fixture(scope="module")
def monkey_mod():
    # using internal API, `monkeypatch` is a `function` scoped fixture
    from _pytest.monkeypatch import MonkeyPatch

    patch = MonkeyPatch()
    yield patch
    patch.undo()


@pytest.fixture(scope="module")
def temp_repo(tmp_path_factory, make_tmp_dir, monkey_mod):
    path = tmp_path_factory.mktemp("temp-repo")
    monkey_mod.chdir(path)
    return make_tmp_dir(path, scm=True, dvc=True)


@pytest.fixture(scope="module")
def repo_tree(temp_repo):
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
        "models/transform.pickle": "model model",  # file
    }

    temp_repo.scm_gen(fs_structure, commit="repo init")
    temp_repo.dvc_gen(dvc_structure, commit="use dvc")

    yield RepoTree(temp_repo.dvc, fetch=True, subrepos=True)


def test_metadata_not_existing(repo_tree):
    path = PathInfo("path") / "that" / "does" / "not" / "exist"

    with pytest.raises(FileNotFoundError):
        repo_tree.metadata(path)


@pytest.mark.parametrize(
    "path",
    [
        "README.md",
        "models/train.py",
        "models/test.py",
        PathInfo("src") / "utils" / "__init__.py",
        PathInfo("src") / "utils" / "serve_model.py",
    ],
)
def test_metadata_git_tracked_file(repo_tree, path):
    root = PathInfo(repo_tree.root_dir)
    meta = repo_tree.metadata(path)

    assert meta.path_info == root / path
    assert meta.repo.root_dir == repo_tree.root_dir
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
        (PathInfo("data") / "raw" / "raw-1.csv", [PathInfo("data")], False),
        (PathInfo("data") / "raw" / "raw-2.csv", [PathInfo("data")], False),
        (
            PathInfo("data") / "processed" / "processed-1.csv",
            [PathInfo("data")],
            False,
        ),
        (
            PathInfo("data") / "processed" / "processed-2.csv",
            [PathInfo("data")],
            False,
        ),
        (
            "models/transform.pickle",
            [PathInfo("models") / "transform.pickle"],
            True,
        ),
    ],
)
def test_metadata_dvc_tracked_file(repo_tree, path, outs, is_output):
    root = PathInfo(repo_tree.root_dir)
    meta = repo_tree.metadata(path)

    assert meta.path_info == root / path
    assert meta.repo.root_dir == repo_tree.root_dir
    assert meta.is_output == is_output
    assert meta.part_of_output != is_output
    assert not meta.contains_outputs
    assert meta.is_dvc
    assert meta.output_exists
    assert not meta.isdir
    assert not meta.is_exec
    assert meta.isfile
    assert {out.path_info for out in meta.outs} == {root / out for out in outs}


@pytest.mark.parametrize("path", ["src", "src/utils"])
def test_metadata_git_only_dirs(repo_tree, path):
    root = PathInfo(repo_tree.root_dir)
    meta = repo_tree.metadata(path)

    assert meta.path_info == root / path
    assert meta.repo.root_dir == repo_tree.root_dir
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
        (".", [PathInfo("data"), PathInfo("models") / "transform.pickle"]),
        ("models", [PathInfo("models") / "transform.pickle"]),
    ],
)
def test_metadata_git_dvc_mixed_dirs(repo_tree, path, expected_outs):
    root = PathInfo(repo_tree.root_dir)
    meta = repo_tree.metadata(root / path)

    assert meta.path_info == root / path
    assert meta.repo.root_dir == repo_tree.root_dir
    assert not meta.is_output
    assert not meta.part_of_output
    assert meta.contains_outputs
    assert not meta.is_dvc
    assert meta.output_exists
    assert meta.isdir
    assert not meta.is_exec
    assert not meta.isfile

    assert {out.path_info for out in meta.outs} == {
        root / out for out in expected_outs
    }


@pytest.mark.parametrize(
    "path, is_output",
    [
        ("data", True),
        ("data/raw", False),  # is inside output
        ("data/processed", False),
    ],
)
def test_metadata_dvc_only_dirs(repo_tree, path, is_output):
    data = PathInfo(repo_tree.root_dir) / "data"
    root = PathInfo(repo_tree.root_dir)
    meta = repo_tree.metadata(root / path)

    assert meta.path_info == root / path
    assert meta.repo.root_dir == repo_tree.root_dir
    assert meta.is_output == is_output
    assert meta.part_of_output != is_output
    assert not meta.contains_outputs
    assert meta.is_dvc
    assert meta.output_exists
    assert meta.isdir
    assert not meta.is_exec
    assert not meta.isfile
    assert {out.path_info for out in meta.outs} == {data}


def test_metadata_on_subrepos(make_tmp_dir, temp_repo, repo_tree):
    subrepo = temp_repo / "subrepo"
    make_subrepo(subrepo, temp_repo.scm)
    subrepo.scm_gen("foo", "foo", commit="add foo on subrepo")
    subrepo.dvc_gen("foobar", "foobar", commit="add foobar on subrepo")

    for path in ["subrepo", "subrepo/foo", "subrepo/foobar"]:
        meta = repo_tree.metadata(temp_repo / path)
        assert meta.repo.root_dir == str(
            subrepo
        ), f"repo root didn't match for {path}"

    # supports external outputs on top-level DVC repo
    external_dir = make_tmp_dir("external-output")
    external_dir.gen("bar", "bar")
    temp_repo.dvc.add(str(external_dir / "bar"), external=True)
    meta = repo_tree.metadata(external_dir / "bar")
    assert meta.repo.root_dir == str(temp_repo)

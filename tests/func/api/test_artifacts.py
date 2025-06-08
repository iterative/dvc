from os.path import join, normpath

import pytest

from dvc.api import artifacts_show
from dvc.testing.tmp_dir import make_subrepo
from dvc.utils import as_posix
from tests.func.artifacts.test_artifacts import get_tag_and_name, make_artifact


@pytest.mark.parametrize("sub", ["sub", ""])
def test_artifacts_show(tmp_dir, dvc, scm, sub):
    subdir = tmp_dir / sub

    dirname = str(subdir.relative_to(tmp_dir))
    tag, name = get_tag_and_name(as_posix(dirname), "myart", "v2.0.0")
    make_artifact(tmp_dir, "myart", tag, subdir / "myart.pkl")

    assert artifacts_show(name) == {
        "path": normpath(join(dirname, "myart.pkl")),
        "rev": scm.get_rev(),
    }
    assert artifacts_show(name, repo=tmp_dir.fs_path) == {
        "path": normpath(join(dirname, "myart.pkl")),
        "rev": scm.get_rev(),
    }
    assert artifacts_show(name, repo=f"file://{tmp_dir.as_posix()}") == {
        "path": normpath(join(dirname, "myart.pkl")),
        "rev": scm.get_rev(),
    }

    assert artifacts_show(name, repo=subdir.fs_path) == {
        "path": normpath(join(dirname, "myart.pkl")),
        "rev": scm.get_rev(),
    }
    with subdir.chdir():
        assert artifacts_show(name) == {
            "path": normpath(join(dirname, "myart.pkl")),
            "rev": scm.get_rev(),
        }


@pytest.mark.parametrize("sub", ["sub", ""])
def test_artifacts_show_subrepo(tmp_dir, scm, sub):
    subrepo = tmp_dir / "subrepo"
    make_subrepo(subrepo, scm)
    subdir = subrepo / sub

    dirname = str(subdir.relative_to(tmp_dir))
    tag, name = get_tag_and_name(as_posix(dirname), "myart", "v2.0.0")
    make_artifact(subrepo, "myart", tag, subdir / "myart.pkl")

    assert artifacts_show(name) == {
        "path": join(dirname, "myart.pkl"),
        "rev": scm.get_rev(),
    }
    assert artifacts_show(name, repo=tmp_dir.fs_path) == {
        "path": join(dirname, "myart.pkl"),
        "rev": scm.get_rev(),
    }
    assert artifacts_show(name, repo=f"file://{tmp_dir.as_posix()}") == {
        "path": join(dirname, "myart.pkl"),
        "rev": scm.get_rev(),
    }

    assert artifacts_show(name, repo=subdir.fs_path) == {
        "path": str((subdir / "myart.pkl").relative_to(subrepo)),
        "rev": scm.get_rev(),
    }
    with subdir.chdir():
        assert artifacts_show(name) == {
            "path": str((subdir / "myart.pkl").relative_to(subrepo)),
            "rev": scm.get_rev(),
        }

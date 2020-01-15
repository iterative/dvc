import filecmp
import os
import shutil
import pytest

from dvc.exceptions import UpdateWithRevNotPossibleError
from dvc.external_repo import clean_repos
from dvc.repo import Repo
from dvc.compat import fspath
from dvc.stage import Stage


@pytest.mark.parametrize("cached", [True, False])
def test_update_import(tmp_dir, dvc, erepo_dir, cached):
    old_rev = None
    with erepo_dir.branch("branch", new=True), erepo_dir.chdir():
        gen = erepo_dir.dvc_gen if cached else erepo_dir.scm_gen
        gen("version", "branch", "add version file")
        old_rev = erepo_dir.scm.get_rev()

    stage = dvc.imp(fspath(erepo_dir), "version", "version", rev="branch")

    imported = tmp_dir / "version"
    assert imported.is_file()
    assert imported.read_text() == "branch"
    assert stage.deps[0].def_repo == {
        "url": fspath(erepo_dir),
        "rev": "branch",
        "rev_lock": old_rev,
    }

    new_rev = None
    with erepo_dir.branch("branch", new=False), erepo_dir.chdir():
        gen = erepo_dir.dvc_gen if cached else erepo_dir.scm_gen
        gen("version", "updated", "update version content")
        new_rev = erepo_dir.scm.get_rev()

    assert old_rev != new_rev

    # Caching in external repos doesn't see upstream updates within single
    # cli call, so we need to clean the caches to see the changes.
    clean_repos()

    assert dvc.status([stage.path]) == {}
    dvc.update(stage.path)
    assert dvc.status([stage.path]) == {}

    assert imported.is_file()
    assert imported.read_text() == "updated"

    stage = Stage.load(dvc, stage.path)
    assert stage.deps[0].def_repo == {
        "url": fspath(erepo_dir),
        "rev": "branch",
        "rev_lock": new_rev,
    }


def test_update_import_url(tmp_dir, dvc, tmp_path_factory):
    import_src = tmp_path_factory.mktemp("import_url_source")
    src = import_src / "file"
    src.write_text("file content")

    dst = tmp_dir / "imported_file"
    stage = dvc.imp_url(fspath(src), fspath(dst))

    assert dst.is_file()
    assert dst.read_text() == "file content"

    # update data
    src.write_text("updated file content")

    assert dvc.status([stage.path]) == {}
    dvc.update(stage.path)
    assert dvc.status([stage.path]) == {}

    assert dst.is_file()
    assert dst.read_text() == "updated file content"


def test_update_rev(tmp_dir, dvc, erepo_dir, monkeypatch):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.scm.checkout("new_branch", create_new=True)
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo on branch")
        erepo_dir.scm.checkout("master")
        erepo_dir.scm.checkout("new_branch_2", create_new=True)
        erepo_dir.dvc_gen(
            "foo", "foo content 2", commit="create foo on branch"
        )
        erepo_dir.scm.checkout("master")

    stage = dvc.imp(fspath(erepo_dir), "foo", "foo_imported", rev="new_branch")
    dvc.update(stage.path, rev="new_branch_2")

    assert (tmp_dir / "foo_imported").read_text() == "foo content 2"


def test_update_rev_non_git_failure(repo_dir, dvc_repo):
    src = "file"
    dst = src + "_imported"

    shutil.copyfile(repo_dir.FOO, src)

    stage = dvc_repo.imp_url(src, dst)

    with pytest.raises(UpdateWithRevNotPossibleError):
        dvc_repo.update(stage.path, rev="dev")

import pytest
import os
import shutil

from dvc.repo import Repo
from dvc.stage import Stage
from dvc.compat import fspath
from dvc.external_repo import clean_repos


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

    status, = dvc.status([stage.path])["version.dvc"]
    changed_dep, = list(status["changed deps"].items())
    assert changed_dep[0].startswith("version ")
    assert changed_dep[1] == "update available"

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


def test_update_import_after_remote_updates_to_dvc(tmp_dir, dvc, erepo_dir):
    old_rev = None
    with erepo_dir.branch("branch", new=True), erepo_dir.chdir():
        erepo_dir.scm_gen("version", "branch", commit="add version file")
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
        erepo_dir.scm.repo.index.remove("version")
        erepo_dir.dvc_gen("version", "updated")
        erepo_dir.scm.add(["version", "version.dvc"])
        erepo_dir.scm.commit("upgrade to DVC tracking")
        new_rev = erepo_dir.scm.get_rev()

    assert old_rev != new_rev

    # Caching in external repos doesn't see upstream updates within single
    # cli call, so we need to clean the caches to see the changes.
    clean_repos()

    status, = dvc.status([stage.path])["version.dvc"]
    changed_dep, = list(status["changed deps"].items())
    assert changed_dep[0].startswith("version ")
    assert changed_dep[1] == "update available"

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


def test_update_before_and_after_dvc_init(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm.repo.index.remove([".dvc"], r=True)
        shutil.rmtree(".dvc")
        erepo_dir.scm_gen("file", "first version")
        erepo_dir.scm.add(["file"])
        erepo_dir.scm.commit("first version")
        old_rev = erepo_dir.scm.get_rev()

    stage = dvc.imp(fspath(erepo_dir), "file", "file")

    with erepo_dir.chdir():
        Repo.init()
        erepo_dir.scm.repo.index.remove(["file"])
        os.remove("file")
        erepo_dir.dvc_gen("file", "second version")
        erepo_dir.scm.add([".dvc", "file.dvc"])
        erepo_dir.scm.commit("version with dvc")
        new_rev = erepo_dir.scm.get_rev()

    assert old_rev != new_rev

    # Caching in external repos doesn't see upstream updates within single
    # cli call, so we need to clean the caches to see the changes.
    clean_repos()

    assert dvc.status([stage.path]) == {
        "file.dvc": [
            {
                "changed deps": {
                    "file ({})".format(fspath(erepo_dir)): "update available"
                }
            }
        ]
    }

    dvc.update(stage.path)

    assert (tmp_dir / "file").read_text() == "second version"
    assert dvc.status([stage.path]) == {}


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

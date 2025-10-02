from pathlib import Path

import pytest

from dvc.cli import main
from dvc.repo.purge import PurgeError


def test_purge_no_remote_configured_errors(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    with pytest.raises(PurgeError):
        dvc.purge()


def test_purge_no_remote_configured_with_force_warns(tmp_dir, dvc, caplog):
    tmp_dir.dvc_gen("foo", "foo")
    caplog.clear()
    dvc.purge(force=True)
    assert (
        "No default remote configured. Proceeding with purge due to --force"
        in caplog.text
    )


def test_purge_api_removes_file_and_cache(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    assert (tmp_dir / "foo").exists()
    assert Path(stage.outs[0].cache_path).exists()

    dvc.push("foo")  # ensure remote has backup

    dvc.purge()

    # workspace file gone, cache gone, metadata remains
    assert not (tmp_dir / "foo").exists()
    assert not Path(stage.outs[0].cache_path).exists()
    assert (tmp_dir / "foo.dvc").exists()


def test_purge_cli_removes_file_and_cache(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    (stage,) = tmp_dir.dvc_gen("bar", "bar")
    assert (tmp_dir / "bar").exists()
    assert Path(stage.outs[0].cache_path).exists()

    # force will skip check that remote has backup
    assert main(["purge", "--force"]) == 0

    assert not (tmp_dir / "bar").exists()
    assert not Path(stage.outs[0].cache_path).exists()
    assert (tmp_dir / "bar.dvc").exists()


def test_purge_targets_only(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    (stage_dir,) = tmp_dir.dvc_gen({"dir": {"a.txt": "A", "b.txt": "B"}})
    assert (tmp_dir / "dir" / "a.txt").exists()
    assert (tmp_dir / "dir" / "b.txt").exists()

    dvc.purge(targets=[str(tmp_dir / "dir")], force=True)

    assert not (tmp_dir / "dir").exists()
    assert (tmp_dir / "dir.dvc").exists()


def test_purge_recursive(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    tmp_dir.dvc_gen({"nested": {"sub": {"file.txt": "content"}}})
    assert (tmp_dir / "nested" / "sub" / "file.txt").exists()

    dvc.purge(targets=["nested"], recursive=True, force=True)
    assert not (tmp_dir / "nested" / "sub" / "file.txt").exists()


def test_purge_individual_targets(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)

    # Generate two *separate* tracked files
    (stage_a,) = tmp_dir.dvc_gen("a.txt", "A")
    (stage_b,) = tmp_dir.dvc_gen("b.txt", "B")

    assert (tmp_dir / "a.txt").exists()
    assert (tmp_dir / "b.txt").exists()
    assert Path(stage_a.outs[0].cache_path).exists()
    assert Path(stage_b.outs[0].cache_path).exists()

    # Push both so purge passes remote safety
    dvc.push()

    # Purge only a.txt
    dvc.purge(targets=[str(tmp_dir / "a.txt")])

    # a.txt and its cache should be gone, but metadata intact
    assert not (tmp_dir / "a.txt").exists()
    assert not Path(stage_a.outs[0].cache_path).exists()
    assert (tmp_dir / "a.txt.dvc").exists()

    # b.txt and its cache should still exist
    assert (tmp_dir / "b.txt").exists()
    assert Path(stage_b.outs[0].cache_path).exists()
    assert (tmp_dir / "b.txt.dvc").exists()


def test_purge_dry_run_does_not_delete(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    (stage,) = tmp_dir.dvc_gen("baz", "baz")
    cache_path = Path(stage.outs[0].cache_path)

    dvc.purge(dry_run=True, force=True)

    assert (tmp_dir / "baz").exists()
    assert cache_path.exists()


def test_purge_dirty_file_requires_force(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    (tmp_dir / "foo").write_text("modified")

    with pytest.raises(PurgeError):
        dvc.purge()

    dvc.purge(force=True)
    assert not (tmp_dir / "foo").exists()


def test_purge_missing_remote_object_requires_force(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    dvc.push("foo")

    remote = dvc.cloud.get_remote_odb("backup")
    remote.fs.remove(remote.path, recursive=True)  # wipe remote

    with pytest.raises(PurgeError):
        dvc.purge()


def test_purge_missing_remote_object_with_force_warns(
    tmp_dir, dvc, make_remote, caplog
):
    make_remote("backup", default=True)
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    dvc.push("foo")

    remote = dvc.cloud.get_remote_odb("backup")
    remote.fs.remove(remote.path, recursive=True)  # wipe remote

    caplog.clear()
    dvc.purge(force=True)
    assert "Some outputs are not present in the remote cache" in caplog.text

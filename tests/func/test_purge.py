from pathlib import Path

import pytest

from dvc.cli import main
from dvc.repo.purge import PurgeError


def test_purge_api_removes_file_and_cache(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    assert (tmp_dir / "foo").exists()
    assert Path(stage.outs[0].cache_path).exists()

    dvc.purge()

    # workspace file gone, cache gone, metadata remains
    assert not (tmp_dir / "foo").exists()
    assert not Path(stage.outs[0].cache_path).exists()
    assert (tmp_dir / "foo.dvc").exists()


def test_purge_cli_removes_file_and_cache(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen("bar", "bar")
    assert (tmp_dir / "bar").exists()
    assert Path(stage.outs[0].cache_path).exists()

    assert main(["purge", "--force"]) == 0

    assert not (tmp_dir / "bar").exists()
    assert not Path(stage.outs[0].cache_path).exists()
    assert (tmp_dir / "bar.dvc").exists()


def test_purge_targets_only(tmp_dir, dvc):
    (stage_dir,) = tmp_dir.dvc_gen({"dir": {"a.txt": "A", "b.txt": "B"}})
    assert (tmp_dir / "dir" / "a.txt").exists()
    assert (tmp_dir / "dir" / "b.txt").exists()

    # purge the whole dir, not just a subfile
    dvc.purge(targets=[str(tmp_dir / "dir")], force=True)

    assert not (tmp_dir / "dir").exists()
    assert (tmp_dir / "dir.dvc").exists()


def test_purge_recursive(tmp_dir, dvc):
    tmp_dir.dvc_gen({"nested": {"sub": {"file.txt": "content"}}})
    assert (tmp_dir / "nested" / "sub" / "file.txt").exists()

    dvc.purge(targets=["nested"], recursive=True, force=True)
    assert not (tmp_dir / "nested" / "sub" / "file.txt").exists()


def test_purge_dry_run_does_not_delete(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen("baz", "baz")
    cache_path = Path(stage.outs[0].cache_path)

    dvc.purge(dry_run=True, force=True)

    assert (tmp_dir / "baz").exists()
    assert cache_path.exists()


def test_purge_dirty_file_requires_force(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    (tmp_dir / "foo").write_text("modified")

    with pytest.raises(PurgeError):
        dvc.purge()

    # but with --force it succeeds
    dvc.purge(force=True)
    assert not (tmp_dir / "foo").exists()

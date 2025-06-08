import logging
import os

import pytest
from funcy import first

from dvc.dvcfile import LOCK_FILE
from dvc.stage.cache import RunCacheNotSupported, _get_stage_hash
from dvc.utils.fs import remove


def _recurse_count_files(path):
    return len([os.path.join(r, f) for r, _, fs in os.walk(path) for f in fs])


def test_push_pull(tmp_dir, dvc, erepo_dir, run_copy, local_remote):
    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    assert dvc.push(run_cache=True) == 2
    erepo_dir.add_remote(config=local_remote.config)
    with erepo_dir.chdir():
        assert not os.path.exists(erepo_dir.dvc.stage_cache.cache_dir)
        assert erepo_dir.dvc.pull(run_cache=True)["fetched"] == 0
        assert os.listdir(erepo_dir.dvc.stage_cache.cache_dir)


def test_restore(tmp_dir, dvc, run_copy, mocker):
    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")

    mock_restore = mocker.spy(dvc.stage_cache, "restore")
    mock_run = mocker.patch("dvc.stage.run.cmd_run")

    # removing any information that `dvc` could use to re-generate from
    (tmp_dir / "bar").unlink()
    (tmp_dir / LOCK_FILE).unlink()

    (stage,) = dvc.reproduce("copy-foo-bar")

    mock_restore.assert_called_once_with(stage, dry=False)
    mock_run.assert_not_called()
    assert (tmp_dir / "bar").exists()
    assert not (tmp_dir / "foo").unlink()
    assert (tmp_dir / LOCK_FILE).exists()


def test_save(tmp_dir, dvc, run_copy):
    run_cache_dir = dvc.stage_cache.cache_dir
    assert not os.path.exists(run_cache_dir)

    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar")
    assert _recurse_count_files(run_cache_dir) == 1
    assert dvc.stage_cache._load(stage)


def test_do_not_save_on_no_exec_and_dry(tmp_dir, dvc, run_copy):
    run_cache_dir = dvc.stage_cache.cache_dir
    assert not os.path.exists(run_cache_dir)

    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar", no_exec=True)

    assert _recurse_count_files(run_cache_dir) == 0
    assert not dvc.stage_cache._load(stage)

    (stage,) = dvc.reproduce("copy-foo-bar", dry=True)

    assert _recurse_count_files(run_cache_dir) == 0
    assert not dvc.stage_cache._load(stage)


@pytest.mark.parametrize(
    "out_type,run_cache",
    [
        ("metrics_no_cache", True),
        ("plots_no_cache", True),
        ("outs_no_cache", False),
    ],
)
def test_outs_no_cache_deactivate_run_cache(tmp_dir, dvc, out_type, run_cache):
    tmp_dir.gen("foo", "foo")
    dvc.run(
        deps=["foo"],
        cmd="cp foo bar && cp foo goo",
        outs=["goo"],
        name="copy-foo-bar",
        **{out_type: ["bar"]},
    )
    assert os.path.isdir(dvc.stage_cache.cache_dir) == run_cache


def test_memory_for_multiple_runs_of_same_stage(tmp_dir, dvc, run_copy, mocker):
    tmp_dir.gen("foo", "foo")
    assert not os.path.exists(dvc.stage_cache.cache_dir)
    run_copy("foo", "bar", name="copy-foo-bar")
    assert _recurse_count_files(dvc.stage_cache.cache_dir) == 1
    tmp_dir.gen("foo", "foobar")
    run_copy("foo", "bar", name="copy-foo-bar")
    assert _recurse_count_files(dvc.stage_cache.cache_dir) == 2

    from dvc.stage import run as _run

    mock_restore = mocker.spy(dvc.stage_cache, "restore")
    mocker.spy(dvc.stage_cache, "_load_cache")
    mock_run = mocker.spy(_run, "cmd_run")

    (tmp_dir / "bar").unlink()
    (tmp_dir / LOCK_FILE).unlink()
    (stage,) = dvc.reproduce("copy-foo-bar")

    assert (tmp_dir / LOCK_FILE).exists()
    assert (tmp_dir / "bar").read_text() == "foobar"
    mock_run.assert_not_called()
    mock_restore.assert_called_once_with(stage, dry=False)
    mock_restore.reset_mock()

    (tmp_dir / LOCK_FILE).unlink()
    tmp_dir.gen("foo", "foo")
    dvc.reproduce("copy-foo-bar")

    assert (tmp_dir / "bar").read_text() == "foo"
    mock_run.assert_not_called()
    mock_restore.assert_called_once_with(stage, dry=False)
    assert (tmp_dir / "bar").exists()
    assert not (tmp_dir / "foo").unlink()
    assert (tmp_dir / LOCK_FILE).exists()


def test_newest_entry_is_loaded_for_non_deterministic_stage(tmp_dir, dvc, mocker):
    tmp_dir.gen("foo", "foo")
    assert not os.path.exists(dvc.stage_cache.cache_dir)

    dvc.stage.add(
        name="non-deterministic",
        cmd='python -c "from time import time; print(time())" > bar',
        deps=["foo"],
        outs=["bar"],
    )

    for i in range(4):
        (stage,) = dvc.reproduce("non-deterministic", force=True)
        assert _recurse_count_files(dvc.stage_cache.cache_dir) == i + 1

    key = _get_stage_hash(stage)
    cache_dir = dvc.stage_cache._get_cache_dir(key)
    old_entries = os.listdir(cache_dir)

    (stage,) = dvc.reproduce("non-deterministic", force=True)
    newest_output = (tmp_dir / "bar").read_text()
    newest_entry = first(e for e in os.listdir(cache_dir) if e not in old_entries)

    from dvc.stage import run as _run

    mock_restore = mocker.spy(dvc.stage_cache, "restore")
    mock_load = mocker.spy(dvc.stage_cache, "_load_cache")
    mock_run = mocker.spy(_run, "cmd_run")

    (tmp_dir / "bar").unlink()
    (tmp_dir / LOCK_FILE).unlink()
    (stage,) = dvc.reproduce("non-deterministic")

    assert (tmp_dir / LOCK_FILE).exists()
    assert (tmp_dir / "bar").read_text() == newest_output
    mock_run.assert_not_called()
    mock_restore.assert_called_once_with(stage, dry=False)
    mock_load.assert_called_with(key, newest_entry)


def test_memory_runs_of_multiple_stages(tmp_dir, dvc, run_copy, mocker):
    tmp_dir.gen("foo", "foo")
    assert not os.path.exists(dvc.stage_cache.cache_dir)

    run_copy("foo", "foo.bak", name="backup-foo")
    assert _recurse_count_files(dvc.stage_cache.cache_dir) == 1

    tmp_dir.gen("bar", "bar")
    run_copy("bar", "bar.bak", name="backup-bar")
    assert _recurse_count_files(dvc.stage_cache.cache_dir) == 2

    from dvc.stage import run as _run

    mock_restore = mocker.spy(dvc.stage_cache, "restore")
    mock_run = mocker.spy(_run, "cmd_run")

    (tmp_dir / "foo.bak").unlink()
    (tmp_dir / "bar.bak").unlink()
    (tmp_dir / LOCK_FILE).unlink()
    (stage,) = dvc.reproduce("backup-foo")

    assert (tmp_dir / "foo.bak").read_text() == "foo"
    assert (tmp_dir / LOCK_FILE).exists()
    mock_run.assert_not_called()
    mock_restore.assert_called_once_with(stage, dry=False)
    mock_restore.reset_mock()

    (stage,) = dvc.reproduce("backup-bar")

    assert (tmp_dir / "bar.bak").read_text() == "bar"
    assert (tmp_dir / LOCK_FILE).exists()
    mock_run.assert_not_called()
    mock_restore.assert_called_once_with(stage, dry=False)


def test_restore_pull(tmp_dir, dvc, run_copy, mocker, local_remote):
    import dvc.output as dvc_output

    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar")

    dvc.push(run_cache=True)

    mock_restore = mocker.spy(dvc.stage_cache, "restore")
    mock_run = mocker.patch("dvc.stage.run.cmd_run")
    mock_checkout = mocker.spy(dvc_output, "checkout")

    # removing any information that `dvc` could use to re-generate from
    (tmp_dir / "bar").unlink()
    (tmp_dir / LOCK_FILE).unlink()
    remove(stage.outs[0].cache_path)

    # removing local run cache
    remove(dvc.stage_cache.cache_dir)

    (stage,) = dvc.reproduce("copy-foo-bar", pull=True)

    mock_restore.assert_called_once_with(stage, pull=True, dry=False)
    mock_run.assert_not_called()
    assert mock_checkout.call_count == 2
    assert (tmp_dir / "bar").exists()
    assert not (tmp_dir / "foo").unlink()
    assert (tmp_dir / LOCK_FILE).exists()


def test_push_pull_unsupported(tmp_dir, dvc, mocker, run_copy, local_remote, caplog):
    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    mocker.patch.object(
        dvc.cloud, "get_remote_odb", side_effect=RunCacheNotSupported("foo")
    )
    with caplog.at_level(logging.DEBUG):
        dvc.push(run_cache=True)
        assert "failed to push run cache" in caplog.text
    with caplog.at_level(logging.DEBUG):
        dvc.pull(run_cache=True)
        assert "failed to pull run cache" in caplog.text

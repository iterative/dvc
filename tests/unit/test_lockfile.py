from dvc.stage import PipelineStage
from dvc import lockfile
import json
import pytest


def test_stage_dump_no_outs_deps(tmp_dir, dvc):
    stage = PipelineStage(name="s1", repo=dvc, path="path", cmd="command")

    lockfile.dump(dvc, "path.lock", stage)
    assert lockfile.load(dvc, "path.lock") == {
        "s1": {"cmd": "command", "deps": {}, "outs": {}}
    }


def test_stage_dump_when_already_exists(tmp_dir, dvc):
    data = {"s1": {"cmd": "command", "deps": {}, "outs": {}}}
    with open("path.lock", "w+") as f:
        json.dump(data, f)
    stage = PipelineStage(name="s2", repo=dvc, path="path", cmd="command2")

    lockfile.dump(dvc, "path.lock", stage)
    assert lockfile.load(dvc, "path.lock") == {
        **data,
        "s2": {"cmd": "command2", "deps": {}, "outs": {}},
    }


def test_stage_dump_with_deps_and_outs(tmp_dir, dvc):
    data = {
        "s1": {
            "cmd": "command",
            "deps": {"1.txt": "checksum"},
            "outs": {"2.txt": "checksum"},
        }
    }
    with open("path.lock", "w+") as f:
        json.dump(data, f)

    stage = PipelineStage(name="s2", repo=dvc, path="path", cmd="command2")
    lockfile.dump(dvc, "path.lock", stage)
    assert lockfile.load(dvc, "path.lock") == {
        **data,
        "s2": {"cmd": "command2", "deps": {}, "outs": {}},
    }


def test_stage_overwrites_if_already_exists(tmp_dir, dvc):
    stage = PipelineStage(name="s2", repo=dvc, path="path", cmd="command2")
    lockfile.dump(dvc, "path.lock", stage)
    stage = PipelineStage(name="s2", repo=dvc, path="path", cmd="command3")
    lockfile.dump(dvc, "path.lock", stage)
    assert lockfile.load(dvc, "path.lock") == {
        "s2": {"cmd": "command3", "deps": {}, "outs": {}},
    }


def test_load_when_lockfile_does_not_exist(tmp_dir, dvc):
    assert {} == lockfile.load(dvc, "dvcfile.lock")


@pytest.mark.parametrize(
    "corrupt_data",
    [
        {"s1": {"cmd": "command", "outs": {}}},
        {"s1": {"outs": {}}},
        {"s1": {"cmd": "command", "deps": {}}},
        {"s1": {}},
        {"s1": {"cmd": "command", "outs": {"file": "checksum"}}},
        {"s1": {"cmd": "command", "deps": {"file": "checksum"}}},
    ],
)
def test_load_when_lockfile_is_corrupted(tmp_dir, dvc, corrupt_data):
    with open("Dvcfile.lock", "w+") as f:
        json.dump(corrupt_data, f)
    with pytest.raises(lockfile.LockfileCorruptedError) as exc_info:
        lockfile.load(dvc, "Dvcfile.lock")
    assert "Dvcfile.lock" in str(exc_info.value)

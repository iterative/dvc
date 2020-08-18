import pytest

from dvc.dvcfile import Lockfile, LockfileCorruptedError
from dvc.stage import PipelineStage
from dvc.utils.serialize import dump_yaml


def test_stage_dump_no_outs_deps(tmp_dir, dvc):
    stage = PipelineStage(name="s1", repo=dvc, path="path", cmd="command")
    lockfile = Lockfile(dvc, "path.lock")
    lockfile.dump(stage)
    assert lockfile.load() == {"s1": {"cmd": "command"}}


def test_stage_dump_when_already_exists(tmp_dir, dvc):
    data = {"s1": {"cmd": "command", "deps": [], "outs": []}}
    dump_yaml("path.lock", data)
    stage = PipelineStage(name="s2", repo=dvc, path="path", cmd="command2")
    lockfile = Lockfile(dvc, "path.lock")
    lockfile.dump(stage)
    assert lockfile.load() == {
        **data,
        "s2": {"cmd": "command2"},
    }


def test_stage_dump_with_deps_and_outs(tmp_dir, dvc):
    data = {
        "s1": {
            "cmd": "command",
            "deps": [{"md5": "1.txt", "path": "checksum"}],
            "outs": [{"md5": "2.txt", "path": "checksum"}],
        }
    }
    dump_yaml("path.lock", data)
    lockfile = Lockfile(dvc, "path.lock")
    stage = PipelineStage(name="s2", repo=dvc, path="path", cmd="command2")
    lockfile.dump(stage)
    assert lockfile.load() == {
        **data,
        "s2": {"cmd": "command2"},
    }


def test_stage_overwrites_if_already_exists(tmp_dir, dvc):
    lockfile = Lockfile(dvc, "path.lock",)
    stage = PipelineStage(name="s2", repo=dvc, path="path", cmd="command2")
    lockfile.dump(stage)
    stage = PipelineStage(name="s2", repo=dvc, path="path", cmd="command3")
    lockfile.dump(stage)
    assert lockfile.load() == {
        "s2": {"cmd": "command3"},
    }


def test_load_when_lockfile_does_not_exist(tmp_dir, dvc):
    assert {} == Lockfile(dvc, "pipelines.lock").load()


@pytest.mark.parametrize(
    "corrupt_data",
    [
        {"s1": {"outs": []}},
        {"s1": {}},
        {
            "s1": {
                "cmd": "command",
                "outs": [
                    {"md5": "checksum", "path": "path", "random": "value"}
                ],
            }
        },
        {"s1": {"cmd": "command", "deps": [{"md5": "checksum"}]}},
    ],
)
def test_load_when_lockfile_is_corrupted(tmp_dir, dvc, corrupt_data):
    dump_yaml("Dvcfile.lock", corrupt_data)
    lockfile = Lockfile(dvc, "Dvcfile.lock")
    with pytest.raises(LockfileCorruptedError) as exc_info:
        lockfile.load()
    assert "Dvcfile.lock" in str(exc_info.value)

import logging
import os

import pytest
from funcy import first
from voluptuous import MultipleInvalid, Schema

from dvc.fs import RemoteMissingDepsError
from dvc.ignore import _no_match
from dvc.output import CHECKSUM_SCHEMA, Output
from dvc.stage import Stage
from dvc.utils.fs import remove


def test_save_missing(dvc, mocker):
    stage = Stage(dvc)
    out = Output(stage, "path", cache=False)
    mocker.patch.object(out.fs, "exists", return_value=False)
    with pytest.raises(out.DoesNotExistError):
        out.save()


@pytest.mark.parametrize(
    "value,expected",
    [
        ("", None),
        (None, None),
        (11111, "11111"),
        ("11111", "11111"),
        ("aAaBa", "aaaba"),
        (
            "3cc286c534a71504476da009ed174423",
            "3cc286c534a71504476da009ed174423",
        ),  # md5
        (
            "d41d8cd98f00b204e9800998ecf8427e-38",
            "d41d8cd98f00b204e9800998ecf8427e-38",
        ),  # etag
        (
            "000002000000000000000000c16859d1d071c6b1ffc9c8557d4909f1",
            "000002000000000000000000c16859d1d071c6b1ffc9c8557d4909f1",
        ),  # hdfs checksum
        # Not much we can do about hex and oct values without writing our own
        # parser. So listing these test cases just to acknowledge this.
        # See https://github.com/iterative/dvc/issues/3331.
        (0x3451, "13393"),
        (0o1244, "676"),
    ],
)
def test_checksum_schema(value, expected):
    assert Schema(CHECKSUM_SCHEMA)(value) == expected


@pytest.mark.parametrize("value", ["1", "11", {}, {"a": "b"}, [], [1, 2]])
def test_checksum_schema_fail(value):
    with pytest.raises(MultipleInvalid):
        assert Schema(CHECKSUM_SCHEMA)(value)


@pytest.mark.parametrize(
    "exists, expected_message",
    [
        (
            False,
            (
                "Output 'path'(stage: 'stage.dvc') is missing version info. "
                "Cache for it will not be collected. "
                "Use `dvc repro` to get your pipeline up to date."
            ),
        ),
        (
            True,
            (
                "Output 'path'(stage: 'stage.dvc') is missing version info. "
                "Cache for it will not be collected. "
                "Use `dvc repro` to get your pipeline up to date.\n"
                "You can also use `dvc commit stage.dvc` to associate "
                "existing 'path' with stage: 'stage.dvc'."
            ),
        ),
    ],
)
def test_get_used_objs(exists, expected_message, mocker, caplog):
    stage = mocker.MagicMock()
    mocker.patch.object(stage, "__str__", return_value="stage: 'stage.dvc'")
    mocker.patch.object(stage, "addressing", "stage.dvc")
    mocker.patch.object(stage, "wdir", os.getcwd())
    mocker.patch.object(stage.repo, "root_dir", os.getcwd())
    mocker.patch.object(stage.repo.dvcignore, "is_ignored", return_value=False)
    mocker.patch.object(
        stage.repo.dvcignore, "check_ignore", return_value=_no_match("path")
    )
    stage.repo.fs.version_aware = False
    stage.repo.fs.PARAM_CHECKSUM = "md5"

    output = Output(stage, "path")

    mocker.patch.object(output, "use_cache", True)
    mocker.patch.object(stage, "is_repo_import", False)
    mocker.patch.object(
        Output, "exists", new_callable=mocker.PropertyMock
    ).return_value = exists

    with caplog.at_level(logging.WARNING, logger="dvc"):
        assert output.get_used_objs() == {}
    assert first(caplog.messages) == expected_message


def test_remote_missing_dependency_on_dir_pull(tmp_dir, scm, dvc, mocker):
    tmp_dir.dvc_gen({"dir": {"subfile": "file2 content"}}, commit="add dir")
    with dvc.config.edit() as conf:
        conf["remote"]["s3"] = {"url": "s3://bucket/name"}
        conf["core"] = {"remote": "s3"}

    remove("dir")
    remove(dvc.cache.local.path)

    mocker.patch(
        "dvc.data_cloud.DataCloud.get_remote",
        side_effect=RemoteMissingDepsError(dvc.fs, "azure", "azure://", []),
    )
    with pytest.raises(RemoteMissingDepsError):
        dvc.pull()


def test_hash_info_cloud_versioning_dir(mocker):
    stage = mocker.MagicMock()
    stage.repo.fs.version_aware = False
    stage.repo.fs.PARAM_CHECKSUM = "etag"
    files = [
        {
            "size": 3,
            "version_id": "WYRG4BglP7pD.gEoJP6a4AqOhl.FRA.h",
            "etag": "acbd18db4cc2f85cedef654fccc4a4d8",
            "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
            "relpath": "bar",
        },
        {
            "size": 3,
            "version_id": "0vL53tFVY5vVAoJ4HG2jCS1mEcohDPE0",
            "etag": "acbd18db4cc2f85cedef654fccc4a4d8",
            "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
            "relpath": "foo",
        },
    ]
    out = Output(stage, "path", files=files)
    # `hash_info`` and `meta`` constructed from `files``
    assert out.hash_info.name == "md5"
    assert out.hash_info.value == "77e8000f532886eef8ee1feba82e9bad.dir"
    assert out.meta.isdir
    assert out.meta.nfiles == 2
    assert out.meta.size == 6


def test_dumpd_cloud_versioning_dir(mocker):
    stage = mocker.MagicMock()
    stage.repo.fs.version_aware = False
    stage.repo.fs.PARAM_CHECKSUM = "md5"
    files = [
        {
            "size": 3,
            "version_id": "WYRG4BglP7pD.gEoJP6a4AqOhl.FRA.h",
            "etag": "acbd18db4cc2f85cedef654fccc4a4d8",
            "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
            "relpath": "bar",
        },
        {
            "size": 3,
            "version_id": "0vL53tFVY5vVAoJ4HG2jCS1mEcohDPE0",
            "etag": "acbd18db4cc2f85cedef654fccc4a4d8",
            "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
            "relpath": "foo",
        },
    ]
    out = Output(stage, "path", files=files)

    dumpd = out.dumpd()
    assert dumpd == {"path": "path", "hash": "md5", "files": files}


def test_version_aware_is_set_based_on_files(mocker):
    import dvc.fs as dvc_fs

    get_fs_config = mocker.spy(dvc_fs, "get_fs_config")

    stage = mocker.MagicMock()
    stage.repo.fs.version_aware = False
    stage.repo.fs.PARAM_CHECKSUM = "etag"
    files = [
        {
            "size": 3,
            "version_id": "WYRG4BglP7pD.gEoJP6a4AqOhl.FRA.h",
            "etag": "acbd18db4cc2f85cedef654fccc4a4d8",
            "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
            "relpath": "bar",
        }
    ]
    Output(stage, "path", files=files)
    # version_aware is passed as `True` if `files` is present`.
    # This will be intentionally ignored in filesystems that don't handle it
    # in `_prepare_credentials`.
    assert get_fs_config.call_args_list[0][1] == {"url": "path", "version_aware": True}

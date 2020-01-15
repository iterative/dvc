import os

import mock

from dvc.exceptions import DvcException
from dvc.remote.local import RemoteLOCAL
from tests.utils import trees_equal


def test_dont_fail_on_unpacked_create_fail(tmp_dir, dvc):
    stage, = tmp_dir.dvc_gen({"dir": {"file": "file_content"}})

    with mock.patch.object(
        RemoteLOCAL, "_create_unpacked_dir", side_effect=DvcException("msg")
    ) as unpacked_create_spy, dvc.state:
        assert not dvc.cache.local.changed_cache(stage.outs[0].checksum)
    assert unpacked_create_spy.call_count == 1


def test_remove_unpacked_on_create_fail(tmp_dir, dvc):
    stage, = tmp_dir.dvc_gen({"dir": {"file": "file_content"}})
    unpacked_dir = stage.outs[0].cache_path + RemoteLOCAL.UNPACKED_DIR_SUFFIX

    # artificial unpacked dir for test purpose
    os.makedirs(unpacked_dir)
    assert os.path.exists(unpacked_dir)

    with mock.patch.object(
        RemoteLOCAL, "_create_unpacked_dir", side_effect=DvcException("msg")
    ), dvc.state:
        assert not dvc.cache.local.changed_cache(stage.outs[0].checksum)

    assert not os.path.exists(unpacked_dir)


def test_create_unpacked_on_status(tmp_dir, dvc):
    stage, = tmp_dir.dvc_gen({"dir": {"file": "file_content"}})
    unpacked_dir = stage.outs[0].cache_path + RemoteLOCAL.UNPACKED_DIR_SUFFIX
    assert not os.path.exists(unpacked_dir)

    with dvc.state:
        assert not dvc.cache.local.changed_cache(stage.outs[0].checksum)
    assert os.path.exists(unpacked_dir)
    trees_equal("dir", unpacked_dir)


def test_dir_cache_changed_on_single_cache_file_modification(tmp_dir, dvc):
    stage, = tmp_dir.dvc_gen(
        {"dir": {"file1": "file1 content", "file2": "file2 content"}}
    )
    unpacked_dir = stage.outs[0].cache_path + RemoteLOCAL.UNPACKED_DIR_SUFFIX
    assert not os.path.exists(unpacked_dir)
    file_md5 = stage.outs[0].dir_cache[0]["md5"]

    with dvc.state:
        assert not dvc.cache.local.changed_cache(stage.outs[0].checksum)
    assert os.path.exists(unpacked_dir)

    cache_file_path = dvc.cache.local.get(file_md5)
    with open(cache_file_path, "a") as fobj:
        fobj.write("modification")

    with dvc.state:
        assert dvc.cache.local.changed_cache(stage.outs[0].checksum)

import os
import stat

import configobj
import pytest

from dvc.cli import main
from dvc.data.db import ODBManager
from dvc.hash_info import HashInfo
from dvc.objects.errors import ObjectFormatError
from dvc.utils import relpath


def test_cache(tmp_dir, dvc):
    cache1_md5 = "123"
    cache2_md5 = "234"
    cache1 = os.path.join(
        dvc.odb.local.cache_dir,
        cache1_md5[0:2],
        cache1_md5[2:],
    )
    cache2 = os.path.join(
        dvc.odb.local.cache_dir,
        cache2_md5[0:2],
        cache2_md5[2:],
    )
    tmp_dir.gen({cache1: "1", cache2: "2"})

    assert os.path.exists(cache1)
    assert os.path.exists(cache2)

    odb = ODBManager(dvc)

    md5_list = list(odb.local.all())
    assert len(md5_list) == 2
    assert cache1_md5 in md5_list
    assert cache2_md5 in md5_list

    odb_cache1 = odb.local.hash_to_path(cache1_md5)
    odb_cache2 = odb.local.hash_to_path(cache2_md5)
    assert os.fspath(odb_cache1) == cache1
    assert os.fspath(odb_cache2) == cache2


def test_cache_load_bad_dir_cache(tmp_dir, dvc):
    from dvc.data import load

    dir_hash = "123.dir"
    fname = os.fspath(dvc.odb.local.hash_to_path(dir_hash))
    tmp_dir.gen({fname: "<clearly>not,json"})
    with pytest.raises(ObjectFormatError):
        load(dvc.odb.local, HashInfo("md5", dir_hash))

    dir_hash = "234.dir"
    fname = os.fspath(dvc.odb.local.hash_to_path(dir_hash))
    tmp_dir.gen({fname: '{"a": "b"}'})
    with pytest.raises(ObjectFormatError):
        load(dvc.odb.local, HashInfo("md5", dir_hash))


def test_external_cache_dir(tmp_dir, dvc, make_tmp_dir):
    cache_dir = make_tmp_dir("cache")

    with dvc.config.edit() as conf:
        conf["cache"]["dir"] = cache_dir.fs_path
    assert not os.path.exists(dvc.odb.local.cache_dir)
    dvc.odb = ODBManager(dvc)

    tmp_dir.dvc_gen({"foo": "foo"})

    tmp_dir.dvc_gen(
        {
            "data_dir": {
                "data": "data_dir/data",
                "data_sub_dir": {"data_sub": "data_dir/data_sub_dir/data_sub"},
            }
        }
    )

    assert not os.path.exists(".dvc/cache")
    assert len(os.listdir(cache_dir)) != 0


def test_remote_cache_references(tmp_dir, dvc):
    with dvc.config.edit() as conf:
        conf["remote"]["storage"] = {"url": "ssh://user@localhost:23"}
        conf["remote"]["cache"] = {"url": "remote://storage/tmp"}
        conf["cache"]["ssh"] = "cache"

    dvc.odb = ODBManager(dvc)

    assert dvc.odb.ssh.fs_path == "/tmp"


def test_shared_cache_dir(tmp_dir):
    cache_dir = os.path.abspath(os.path.join(os.curdir, "cache"))
    for d in ["dir1", "dir2"]:
        os.mkdir(d)
        with (tmp_dir / d).chdir():
            ret = main(["init", "--no-scm"])
            assert ret == 0

            ret = main(["config", "cache.dir", cache_dir])
            assert ret == 0

            assert not os.path.exists(os.path.join(".dvc", "cache"))

            (tmp_dir / d).gen({"common": "common", "unique": d})

            ret = main(["add", "common", "unique"])
            assert ret == 0

    assert not os.path.exists(os.path.join("dir1", ".dvc", "cache"))
    assert not os.path.exists(os.path.join("dir2", ".dvc", "cache"))

    subdirs = list(
        filter(
            lambda x: os.path.isdir(os.path.join(cache_dir, x)),
            os.listdir(cache_dir),
        )
    )
    assert len(subdirs) == 3
    assert len(os.listdir(os.path.join(cache_dir, subdirs[0]))) == 1

    assert len(os.listdir(os.path.join(cache_dir, subdirs[1]))) == 1
    assert len(os.listdir(os.path.join(cache_dir, subdirs[2]))) == 1


def test_cache_link_type(tmp_dir, scm, dvc):
    with dvc.config.edit() as conf:
        conf["cache"]["type"] = "reflink,copy"
    dvc.odb = ODBManager(dvc)

    stages = tmp_dir.dvc_gen({"foo": "foo"})
    assert len(stages) == 1
    assert (tmp_dir / "foo").read_text().strip() == "foo"


def test_cmd_cache_dir(tmp_dir, scm, dvc):
    ret = main(["cache", "dir"])
    assert ret == 0


def test_cmd_cache_abs_path(tmp_dir, scm, dvc, make_tmp_dir):
    cache_dir = make_tmp_dir("cache")
    ret = main(["cache", "dir", cache_dir.fs_path])
    assert ret == 0

    config = configobj.ConfigObj(dvc.config.files["repo"])
    assert config["cache"]["dir"] == cache_dir.fs_path


def test_cmd_cache_relative_path(tmp_dir, scm, dvc, make_tmp_dir):
    cache_dir = make_tmp_dir("cache")
    dname = relpath(cache_dir)
    ret = main(["cache", "dir", dname])
    assert ret == 0

    dvc.config.load()
    dvc.odb = ODBManager(dvc)

    # NOTE: we are in the repo's root and config is in .dvc/, so
    # dir path written to config should be just one level above.
    rel = os.path.join("..", dname)
    config = configobj.ConfigObj(dvc.config.files["repo"])
    assert config["cache"]["dir"] == rel.replace("\\", "/")

    tmp_dir.dvc_gen({"foo": "foo"})

    subdirs = os.listdir(cache_dir)
    assert len(subdirs) == 1
    files = os.listdir(os.path.join(cache_dir, subdirs[0]))
    assert len(files) == 1


def test_default_cache_type(dvc):
    assert dvc.odb.local.cache_types == ["reflink", "copy"]


@pytest.mark.skipif(os.name == "nt", reason="Not supported for Windows.")
@pytest.mark.parametrize("group", [False, True])
def test_shared_cache(tmp_dir, dvc, group):
    from dvc.utils.fs import umask

    if group:
        with dvc.config.edit() as conf:
            conf["cache"].update({"shared": "group"})
    dvc.odb = ODBManager(dvc)
    cache_dir = dvc.odb.local.cache_dir

    assert not os.path.exists(cache_dir)

    tmp_dir.dvc_gen(
        {"file": "file content", "dir": {"file2": "file 2 " "content"}}
    )

    actual = {}
    for root, dnames, fnames in os.walk(cache_dir):
        for name in dnames + fnames:
            path = os.path.join(root, name)
            actual[path] = oct(stat.S_IMODE(os.stat(path).st_mode))

    file_mode = oct(0o444)
    dir_mode = oct(0o2775 if group else (0o777 & ~umask))

    expected = {
        os.path.join(cache_dir, "17"): dir_mode,
        os.path.join(
            cache_dir, "17", "4eaa1dd94050255b7b98a7e1924b31.dir"
        ): file_mode,
        os.path.join(cache_dir, "97"): dir_mode,
        os.path.join(
            cache_dir, "97", "e17781c198500e2766ea56bd697c03"
        ): file_mode,
        os.path.join(cache_dir, "d1"): dir_mode,
        os.path.join(
            cache_dir, "d1", "0b4c3ff123b26dc068d43a8bef2d23"
        ): file_mode,
    }

    assert expected == actual

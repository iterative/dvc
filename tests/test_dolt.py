import os
import shutil

import pytest

import doltcli as dolt

from dvc.remote.base import Remote
from dvc.remote.index import RemoteIndex
from dvc.remote.local import LocalRemote
from dvc.repo import Repo as DvcRepo


def match_files(files, expected_files):
    left = {(f["path"], f["isout"], f["isdir"]) for f in files}
    right = {(os.path.join(*args), isout, isdir) for (args, isout, isdir) in expected_files}
    assert left == right


@pytest.fixture
def empty_doltdb(tmp_dir):
    db_path = os.path.join(tmp_dir, "test_db")
    dolt.Dolt.init(db_path)
    db = dolt.Dolt(db_path)
    return db


@pytest.fixture()
def doltdb(empty_doltdb):
    TEST_TABLE = "t1"
    empty_doltdb.sql(query=f'''
        CREATE TABLE `{TEST_TABLE}` (
            `name` VARCHAR(32),
            `id` INT NOT NULL,
            PRIMARY KEY (`id`)
        );
    ''')
    empty_doltdb.sql("insert into t1 values ('connie', 0)")
    empty_doltdb.add(TEST_TABLE)
    empty_doltdb.commit('Created test table')
    return empty_doltdb


def test_dolt_dir_add(tmp_dir, dvc, doltdb):
    from dvc.objects import load

    (stage,) = tmp_dir.dvc_add(doltdb.repo_dir)

    assert stage is not None
    assert len(stage.deps) == 0
    assert len(stage.outs) == 1

    hash_info = stage.outs[0].hash_info
    assert hash_info.value == f"{doltdb.head}.dolt"

    dir_info = load(dvc.odb.local, hash_info).hash_info.dir_info
    for path, _ in dir_info.trie.items():
        assert "\\" not in path

    assert hash_info.value == f"{doltdb.head}.dolt"


def test_dolt_dir_checkout_branch(tmp_dir, dvc, doltdb):
    first_commit = doltdb.head

    #switch to new branch, add commit
    doltdb.checkout("tmp_br", checkout_branch=True)
    doltdb.sql("insert into t1 values ('bob', '1'), ('sally', 2)")
    doltdb.add("t1")
    doltdb.commit("Add rows")
    second_commit = doltdb.head

    # dvc file for new branch
    (_,) = tmp_dir.dvc_add(doltdb.repo_dir)

    # switch back to master
    doltdb.checkout("master")
    assert first_commit == doltdb.head

    # expect dvc checkout to call `dolt checkout tmp_br`
    dvc.checkout(force=True)

    assert second_commit == doltdb.head


def test_dolt_dir_checkout_state(tmp_dir, dvc, doltdb):
    #switch to new branch, add commit
    doltdb.checkout("tmp_br", checkout_branch=True)
    doltdb.sql("insert into t1 values ('bob', '1'), ('sally', 2)")
    doltdb.add("t1")
    doltdb.commit("Add rows")

    # dvc file for new branch
    (_,) = tmp_dir.dvc_add(doltdb.repo_dir)

    # switch back to master
    doltdb.sql("drop table t1")
    # expect dvc checkout to restore table
    dvc.checkout(force=True)

    res = doltdb.sql("select * from t1", result_format="csv")
    assert [d for d in res if d["name"] == "connie"][0]["id"] == "0"

def test_dolt_dir_status(tmp_dir, dvc, doltdb):
    # mismatch between working and stored head

    first_commit = doltdb.head

    #switch to new branch, add commit
    doltdb.checkout("tmp_br", checkout_branch=True)
    doltdb.sql("insert into t1 values ('bob', '1'), ('sally', 2)")
    doltdb.add("t1")
    doltdb.commit("Add rows")

    # dvc file for new branch
    (_,) = tmp_dir.dvc_add(doltdb.repo_dir)

    status = dvc.status(targets=["test_db"])
    assert status == {}

    # switch back to master
    doltdb.checkout("master")
    assert first_commit == doltdb.head

    status = dvc.status(targets=["test_db"])
    assert status == {'test_db.dvc': [{'changed outs': {'test_db': 'modified'}}]}


def test_dolt_dir_remove(tmp_dir, dvc, doltdb):

    # dvc file for new branch
    (_,) = tmp_dir.dvc_add(doltdb.repo_dir)

    files = DvcRepo.ls(os.fspath(tmp_dir))
    exp = (
        ((".dvcignore",), False, False),
        (("test_db",), False, True),
        (("test_db.dvc",), False, False),
    )
    match_files(files, exp)

    dvc.remove(doltdb.repo_dir + ".dvc", outs=True)

    files = DvcRepo.ls(os.fspath(tmp_dir))
    exp = (
        (("test_db",), False, True),
        ((".dvcignore",), False, False),
    )
    match_files(files, exp)


def test_dolt_dir_list(tmp_dir, dvc, doltdb):
    doltdb.checkout("tmp_br", checkout_branch=True)
    doltdb.sql("insert into t1 values ('bob', '1'), ('sally', 2)")
    doltdb.add("t1")
    doltdb.commit("Add rows")

    # dvc file for new branch
    (_,) = tmp_dir.dvc_add(doltdb.repo_dir)

    files = DvcRepo.ls(os.fspath(tmp_dir))
    exp = (
        ((".dvcignore",), False, False),
        (("test_db",), False, True),
        (("test_db.dvc",), False, False),
    )
    match_files(files, exp)


def test_dolt_dir_stage(tmp_dir, dvc, doltdb):
    pass


@pytest.fixture(scope="function")
def remote(tmp_dir, dvc, tmp_path_factory, mocker):
    url = os.fspath(tmp_path_factory.mktemp("upstream"))
    dvc.config["remote"]["upstream"] = {"url": url}
    dvc.config["core"]["remote"] = "upstream"

    # patch hashes_exist since the LocalRemote normally overrides
    # BaseFileSystem.hashes_exist.
    def hashes_exist(self, *args, **kwargs):
        return Remote.hashes_exist(self, *args, **kwargs)

    mocker.patch.object(LocalRemote, "hashes_exist", hashes_exist)

    # patch index class since LocalRemote normally overrides index class
    mocker.patch.object(LocalRemote, "INDEX_CLS", RemoteIndex)

    return dvc.cloud.get_remote("upstream")


def test_dolt_dir_push(tmp_dir, dvc, doltdb, remote):
    remote_url = remote.fs.config.get("url")

    (_,) = tmp_dir.dvc_add(doltdb.repo_dir)
    dvc.push(targets=["test_db"], remote="upstream")

    assert os.path.exists(os.path.join(remote_url, "manifest"))
    assert os.path.exists(os.path.join(remote_url, "lock"))


def test_dolt_dir_pull(tmp_dir, dvc, doltdb, remote):
    (_,) = tmp_dir.dvc_add(doltdb.repo_dir)
    dvc.push(targets=["test_db"], remote="upstream")

    shutil.rmtree(os.path.join(tmp_dir, "test_db"))
    dvc.pull(targets=["test_db"], remote="upstream")

    assert os.path.exists(os.path.join(tmp_dir, "test_db", ".dolt"))


def test_dolt_dir_import(tmp_dir, dvc, doltdb):
    pass


def test_dolt_dir_run(tmp_dir, dvc, doltdb):
    # target db
    target_path = os.path.join(tmp_dir, "target_db")
    os.makedirs(target_path)
    dolt.Dolt.init(target_path)
    target_db = dolt.Dolt(target_path)

    target_db.sql(query=f'''
        CREATE TABLE `t2` (
            `name` VARCHAR(32),
            `id` INT NOT NULL,
            PRIMARY KEY (`id`)
        );
    ''')
    target_db.add("t2")
    target_db.commit("Init t2")

    # script that connects two dolt dbs
    script = """
import sys
import doltcli as dolt
_, source, target = sys.argv

source_db = dolt.Dolt(source)
target_db = dolt.Dolt(target)
rows = source_db.sql("select * from t1", result_format="csv")
dolt.write_rows(target_db, "t2", rows, commit=True, commit_message="Automated row-add")
"""

    script_path = os.path.join(tmp_dir, "script.py")
    with open(script_path, "w") as f:
        f.write(script)

    # dvc add dependencies
    (_,) = tmp_dir.dvc_add(doltdb.repo_dir)
    (_,) = tmp_dir.dvc_add(script_path)

    # create single-stage run
    dvc.run(
        deps=[script_path, doltdb.repo_dir],
        outs=[target_db.repo_dir],
        cmd=f"python {script_path} {doltdb.repo_dir} {target_db.repo_dir}",
        single_stage=True,
    )

    cmp = doltdb.sql("select * from t1", result_format="csv")
    res = target_db.sql("select * from t2", result_format="csv")

    assert cmp == res

import errno
import filecmp
import logging
import os
import shutil
import stat
import textwrap
import time

import colorama
import pytest
from mock import call, patch

import doltcli as dolt
import dvc as dvc_module
from dvc.dvcfile import DVC_FILE_SUFFIX
from dvc.exceptions import (
    DvcException,
    InvalidArgumentError,
    OutputDuplicationError,
    OverlappingOutputPathsError,
    RecursiveAddingWhileUsingFilename,
)
from dvc.fs.local import LocalFileSystem
from dvc.hash_info import HashInfo
from dvc.main import main
from dvc.objects.db import ODBManager
from dvc.output.base import OutputAlreadyTrackedError, OutputIsStageFileError
from dvc.repo import Repo as DvcRepo
from dvc.stage import Stage
from dvc.stage.exceptions import (
    StageExternalOutputsError,
    StagePathNotFoundError,
)
from dvc.system import System
from dvc.utils import LARGE_DIR_SIZE, file_md5, relpath
from dvc.utils.fs import path_isin
from dvc.utils.serialize import YAMLFileCorruptedError, load_yaml
from tests.basic_env import TestDvc
from tests.utils import get_gitignore_content


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

    dir_info = load(dvc.odb.local, hash_info).hash_info.dir_info
    for path, _ in dir_info.trie.items():
        assert "\\" not in path

    assert hash_info.dolt_head == doltdb.head

def test_dolt_dir_checkout(tmp_dir, dvc, doltdb):
    from dvc.objects import load

    first_commit = doltdb.head

    #switch to new branch, add commit
    doltdb.checkout("tmp_br", checkout_branch=True)
    doltdb.sql("insert into t1 values ('bob', '0'), ('sally', 1)")
    doltdb.add("t1")
    doltdb.commit("Add rows")
    second_commit = doltdb.head

    # dvc file for new branch
    (stage,) = tmp_dir.dvc_add(doltdb.repo_dir)
    hash_info = stage.outs[0].hash_info

    # switch back to master
    doltdb.checkout("master")
    assert first_commit == doltdb.head

    # expect dvc checkout to call `dolt checkout tmp_br`
    dvc.checkout(force=True)

    assert second_commit == doltdb.head

def test_dolt_dir_status(tmp_dir, dvc, doltdb):
    pass


def test_dolt_dir_commit(tmp_dir, dvc, doltdb):
    pass

def test_dolt_dir_remove(tmp_dir, dvc, doltdb):
    pass


def test_dolt_dir_list(tmp_dir, dvc, doltdb):
    pass

def test_dolt_dir_stage(tmp_dir, dvc, doltdb):
    pass

def test_dolt_dir_push(tmp_dir, dvc, doltdb):
    pass

def test_dolt_dir_pull(tmp_dir, dvc, doltdb):
    pass

def test_dolt_dir_import(tmp_dir, dvc, doltdb):
    pass

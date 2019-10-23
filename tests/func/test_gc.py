from __future__ import unicode_literals
import os

import pytest
import configobj

from git import Repo

from dvc.utils.compat import pathlib
from dvc.main import main
from dvc.repo import Repo as DvcRepo
from dvc.exceptions import CollectCacheError

from tests.basic_env import TestDvcGit
from tests.basic_env import TestDir


class TestGC(TestDvcGit):
    def setUp(self):
        super(TestGC, self).setUp()

        self.dvc.add(self.FOO)
        self.dvc.add(self.DATA_DIR)
        self.good_cache = [
            self.dvc.cache.local.get(md5) for md5 in self.dvc.cache.local.all()
        ]

        self.bad_cache = []
        for i in ["123", "234", "345"]:
            path = os.path.join(self.dvc.cache.local.cache_dir, i[0:2], i[2:])
            self.create(path, i)
            self.bad_cache.append(path)

    def test_api(self):
        self.dvc.gc()
        self._test_gc()

    def test_cli(self):
        ret = main(["gc", "-f"])
        self.assertEqual(ret, 0)
        self._test_gc()

    def _test_gc(self):
        self.assertTrue(os.path.isdir(self.dvc.cache.local.cache_dir))
        for c in self.bad_cache:
            self.assertFalse(os.path.exists(c))

        for c in self.good_cache:
            self.assertTrue(os.path.exists(c))


class TestGCBranchesTags(TestDvcGit):
    def _check_cache(self, num):
        total = 0
        for root, dirs, files in os.walk(os.path.join(".dvc", "cache")):
            total += len(files)
        self.assertEqual(total, num)

    def test(self):
        fname = "file"

        with open(fname, "w+") as fobj:
            fobj.write("v1.0")

        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)
        self.dvc.scm.add([".gitignore", stages[0].relpath])
        self.dvc.scm.commit("v1.0")
        self.dvc.scm.tag("v1.0")

        self.dvc.scm.checkout("test", create_new=True)
        self.dvc.remove(stages[0].relpath, outs_only=True)
        with open(fname, "w+") as fobj:
            fobj.write("test")
        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)
        self.dvc.scm.add([".gitignore", stages[0].relpath])
        self.dvc.scm.commit("test")

        self.dvc.scm.checkout("master")
        self.dvc.remove(stages[0].relpath, outs_only=True)
        with open(fname, "w+") as fobj:
            fobj.write("trash")
        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)
        self.dvc.scm.add([".gitignore", stages[0].relpath])
        self.dvc.scm.commit("trash")

        self.dvc.remove(stages[0].relpath, outs_only=True)
        with open(fname, "w+") as fobj:
            fobj.write("master")
        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)
        self.dvc.scm.add([".gitignore", stages[0].relpath])
        self.dvc.scm.commit("master")

        self._check_cache(4)

        self.dvc.gc(all_tags=True, all_branches=True)

        self._check_cache(3)

        self.dvc.gc(all_tags=False, all_branches=True)

        self._check_cache(2)

        self.dvc.gc(all_tags=True, all_branches=False)

        self._check_cache(1)


class TestGCMultipleDvcRepos(TestDvcGit):
    def _check_cache(self, num):
        total = 0
        for root, dirs, files in os.walk(os.path.join(".dvc", "cache")):
            total += len(files)
        self.assertEqual(total, num)

    def setUp(self):
        super(TestGCMultipleDvcRepos, self).setUp()
        self.additional_path = TestDir.mkdtemp()
        self.additional_git = Repo.init(self.additional_path)
        self.additional_dvc = DvcRepo.init(self.additional_path)

        cache_path = os.path.join(self._root_dir, ".dvc", "cache")
        config_path = os.path.join(
            self.additional_path, ".dvc", "config.local"
        )
        cfg = configobj.ConfigObj()
        cfg.filename = config_path
        cfg["cache"] = {"dir": cache_path}
        cfg.write()

        self.additional_dvc = DvcRepo(self.additional_path)

    def test(self):

        # ADD FILE ONLY IN MAIN PROJECT
        fname = "only_in_first"
        with open(fname, "w+") as fobj:
            fobj.write("only in main repo")

        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)

        # ADD FILE IN MAIN PROJECT THAT IS ALSO IN SECOND PROJECT
        fname = "in_both"
        with open(fname, "w+") as fobj:
            fobj.write("in both repos")

        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)

        cwd = os.getcwd()
        os.chdir(self.additional_path)
        # ADD FILE ONLY IN SECOND PROJECT
        fname = os.path.join(self.additional_path, "only_in_second")
        with open(fname, "w+") as fobj:
            fobj.write("only in additional repo")

        stages = self.additional_dvc.add(fname)
        self.assertEqual(len(stages), 1)

        # ADD FILE IN SECOND PROJECT THAT IS ALSO IN MAIN PROJECT
        fname = os.path.join(self.additional_path, "in_both")
        with open(fname, "w+") as fobj:
            fobj.write("in both repos")

        stages = self.additional_dvc.add(fname)
        self.assertEqual(len(stages), 1)

        os.chdir(cwd)

        self._check_cache(3)

        self.dvc.gc(repos=[self.additional_path])
        self._check_cache(3)

        self.dvc.gc()
        self._check_cache(2)


def test_all_commits(git, dvc_repo):
    def add_and_commit():
        stages = dvc_repo.add(str(testfile))
        dvc_repo.scm.add([s.relpath for s in stages])
        dvc_repo.scm.commit("message")

    cache_dir = os.path.join(dvc_repo.root_dir, ".dvc", "cache")
    testfile = pathlib.Path("testfile")

    testfile.write_text("uncommited")
    dvc_repo.add(str(testfile))

    testfile.write_text("commited")
    add_and_commit()

    testfile.write_text("modified")
    add_and_commit()

    testfile.write_text("workspace")
    dvc_repo.add(str(testfile))

    N = _count_files(cache_dir)

    dvc_repo.gc(all_commits=True)

    # Only one uncommited file should go away
    assert _count_files(cache_dir) == N - 1


def _count_files(path):
    return sum(len(files) for _, _, files in os.walk(path))


def test_gc_no_dir_cache(repo_dir, dvc_repo):
    dvc_repo.add(repo_dir.FOO)
    dvc_repo.add(repo_dir.BAR)
    dir_stage, = dvc_repo.add(repo_dir.DATA_DIR)

    os.unlink(dir_stage.outs[0].cache_path)

    with pytest.raises(CollectCacheError):
        dvc_repo.gc()

    assert _count_files(dvc_repo.cache.local.cache_dir) == 4

    dvc_repo.gc(force=True)

    assert _count_files(dvc_repo.cache.local.cache_dir) == 2

# encoding: utf-8

from __future__ import unicode_literals

import os
import shutil
import uuid
import tempfile
import logging
import warnings

from git import Repo
from git.exc import GitCommandNotFound
from unittest import TestCase
import pytest

from dvc.command.remote import CmdRemoteAdd
from dvc.repo import Repo as DvcRepo
from dvc.utils.compat import open, str


logger = logging.getLogger("dvc")


class TestDirFixture(object):
    GCP_CREDS_FILE = os.path.abspath(
        os.path.join("scripts", "ci", "gcp-creds.json")
    )
    DATA_DIR = "data_dir"
    DATA_SUB_DIR = os.path.join(DATA_DIR, "data_sub_dir")
    DATA = os.path.join(DATA_DIR, "data")
    DATA_SUB = os.path.join(DATA_SUB_DIR, "data_sub")
    DATA_CONTENTS = DATA
    DATA_SUB_CONTENTS = DATA_SUB
    FOO = "foo"
    FOO_CONTENTS = FOO
    BAR = "bar"
    # NOTE: len(FOO_CONTENTS) must be != len(BAR_CONTENTS)
    #
    # Our state database relies on file mtime and file size to determine
    # that a file has changed. Usually, mtime is enough by itself but on
    # some filesystems like APFS on macOS mtime resolution is so bad,
    # that our tests can overwrite a file in that time window without dvc
    # being able to detect that, thus causing tests to fail. Usually,
    # in tests, we replace foo with bar, so we need to make sure that when we
    # modify a file in our tests, its content length changes.
    BAR_CONTENTS = BAR + "r"
    CODE = "code.py"
    CODE_CONTENTS = (
        "import sys\nimport shutil\n"
        "shutil.copyfile(sys.argv[1], sys.argv[2])"
    )
    UNICODE = "тест"
    UNICODE_CONTENTS = "проверка"

    def __init__(self, root_dir=None):
        if root_dir:
            os.mkdir(root_dir)
        else:
            root_dir = self.mkdtemp()
        self._root_dir = os.path.realpath(root_dir)

    def _pushd(self, d):
        if not hasattr(self, "_saved_dir"):
            self._saved_dir = os.path.realpath(os.curdir)
        os.chdir(d)

    def _popd(self):
        os.chdir(self._saved_dir)
        self._saved_dir = None

    def create(self, name, contents):
        dname = os.path.dirname(name)
        if len(dname) > 0 and not os.path.isdir(dname):
            os.makedirs(dname)

        with open(name, "a", encoding="utf-8") as f:
            f.write(
                contents
                if isinstance(contents, str)
                else contents.decode("utf-8")
            )

    @staticmethod
    def mkdtemp():
        prefix = "dvc-test.{}.".format(os.getpid())
        suffix = ".{}".format(uuid.uuid4())
        return tempfile.mkdtemp(prefix=prefix, suffix=suffix)

    def setUp(self):
        self._pushd(self._root_dir)
        self.create(self.FOO, self.FOO_CONTENTS)
        self.create(self.BAR, self.BAR_CONTENTS)
        self.create(self.CODE, self.CODE_CONTENTS)
        os.mkdir(self.DATA_DIR)
        os.mkdir(self.DATA_SUB_DIR)
        self.create(self.DATA, self.DATA_CONTENTS)
        self.create(self.DATA_SUB, self.DATA_SUB_CONTENTS)
        self.create(self.UNICODE, self.UNICODE_CONTENTS)

    def tearDown(self):
        self._popd()
        try:
            shutil.rmtree(self._root_dir)
        except OSError as exc:
            # We ignore this under Windows with a warning because it happened
            # to be really hard to trace all not properly closed files.
            #
            # Best guess so far is that gitpython is the culprit:
            # it opens files and uses __del__ to close them, which can happen
            # late in current pythons. TestGitFixture and TestDvcFixture try
            # to close that and it works on most of the tests, but not all.
            # Repos and thus git repos are created all over the dvc ;)
            if os.name == "nt" and exc.winerror == 32:
                warnings.warn("Failed to remove test dir: " + str(exc))
            else:
                raise


class TestGitFixture(TestDirFixture):
    N_RETRIES = 5

    def setUp(self):
        super(TestGitFixture, self).setUp()
        # NOTE: handles EAGAIN error on BSD systems (osx in our case).
        # Otherwise when running tests you might get this exception:
        #
        #    GitCommandNotFound: Cmd('git') not found due to:
        #        OSError('[Errno 35] Resource temporarily unavailable')
        retries = self.N_RETRIES
        while retries:
            try:
                self.git = Repo.init()
            except GitCommandNotFound:
                retries -= 1
                continue
            break

        self.git.index.add([self.CODE])
        self.git.index.commit("add code")

    def tearDown(self):
        self.git.close()


class TestGitSubmoduleFixture(TestGitFixture):
    def __init__(self, root_dir=None):
        super(TestGitSubmoduleFixture, self).__init__(root_dir)

    def setUp(self):
        super(TestGitSubmoduleFixture, self).setUp()
        subrepo = Repo.init()
        subrepo_path = "subrepo"
        self.git.create_submodule(subrepo_path, subrepo_path, subrepo.git_dir)
        self._pushd(subrepo_path)


class TestDvcFixture(TestGitFixture):
    def __init__(self, root_dir=None):
        super(TestDvcFixture, self).__init__(root_dir)

    def setUp(self):
        super(TestDvcFixture, self).setUp()
        self.dvc = DvcRepo.init(self._root_dir)
        self.dvc.scm.commit("init dvc")
        logger.setLevel("DEBUG")

    def tearDown(self):
        self.dvc.scm.git.close()


class TestDvcGitInitializedFixture(TestDvcFixture):
    def __init__(self, root_dir=None):
        super(TestDvcGitInitializedFixture, self).__init__(root_dir)

    def setUp(self):
        super(TestDvcGitInitializedFixture, self).setUp()
        self.git.init()


class TestDvcDataFileFixture(TestDvcGitInitializedFixture):
    DATA_DVC_FILE = "data.dvc"
    DATA_DIR_DVC_FILE = "data_sub_dir.dvc"
    REMOTE = "myremote2"

    def __init__(self, root_dir=None, cache_dir=None):
        super(TestDvcDataFileFixture, self).__init__(root_dir)
        self.cache_dir = cache_dir

    def setUp(self):
        super(TestDvcDataFileFixture, self).setUp()

        self.dvc.add(self.DATA)
        self.dvc.add(self.DATA_SUB_DIR)

        if self.cache_dir:
            shutil.copytree(self.dvc.cache.local.cache_dir, self.cache_dir)

            class MockConfig:
                system = None
                glob = None
                local = None
                default = True
                name = self.REMOTE
                url = self.cache_dir

            cmd = CmdRemoteAdd(MockConfig())
            cmd.run()

        self.git.index.add(
            [
                os.path.join(self.DATA_DIR, self.DATA_DVC_FILE),
                os.path.join(self.DATA_DIR, self.DATA_DIR_DVC_FILE),
                os.path.join(self.DATA_DIR, ".gitignore"),
                ".dvc/config",
                self.FOO,
                self.BAR,
            ]
        )
        self.git.index.commit("Hello world commit")

        # Return to the dir we started to not confuse parent fixture
        os.chdir(self._saved_dir)


# NOTE: Inheritance order in the classes below is important.


class TestDir(TestDirFixture, TestCase):
    def __init__(self, methodName, root_dir=None):
        TestDirFixture.__init__(self, root_dir)
        TestCase.__init__(self, methodName)


class TestGit(TestGitFixture, TestCase):
    def __init__(self, methodName, root_dir=None):
        TestGitFixture.__init__(self, root_dir)
        TestCase.__init__(self, methodName)


class TestGitSubmodule(TestGitSubmoduleFixture, TestCase):
    def __init__(self, methodName, root_dir=None):
        TestGitSubmoduleFixture.__init__(self, root_dir)
        TestCase.__init__(self, methodName)


class TestDvc(TestDvcFixture, TestCase):
    def __init__(self, methodName, root_dir=None):
        TestDvcFixture.__init__(self, root_dir)
        TestCase.__init__(self, methodName)

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog


class TestDvcPkg(TestDvcFixture, TestCase):
    GIT_PKG = "git_pkg"
    CACHE_DIR = "mycache"

    def __init__(self, methodName, root_dir=None):
        TestDvcFixture.__init__(self, root_dir)
        TestCase.__init__(self, methodName)

        self.pkg_dir = os.path.join(self._root_dir, self.GIT_PKG)
        cache_dir = os.path.join(self._root_dir, self.CACHE_DIR)
        self.pkg_fixture = TestDvcDataFileFixture(
            root_dir=self.pkg_dir, cache_dir=cache_dir
        )
        self.pkg_fixture.setUp()

import os
import uuid
import tempfile
from git import Repo
from git.exc import GitCommandNotFound
from unittest import TestCase

import dvc.logger as logger
from dvc.repo import Repo as DvcRepo


class TestDir(TestCase):
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

    def _pushd(self, d):
        self._saved_dir = os.path.realpath(os.curdir)
        os.chdir(d)

    def _popd(self):
        os.chdir(self._saved_dir)
        self._saved_dir = None

    def create(self, name, contents):
        dname = os.path.dirname(name)
        if len(dname) > 0 and not os.path.isdir(dname):
            os.makedirs(dname)

        with open(name, "a") as f:
            f.write(contents)

    @staticmethod
    def mkdtemp():
        prefix = "dvc-test.{}.".format(os.getpid())
        suffix = ".{}".format(uuid.uuid4())
        return tempfile.mkdtemp(prefix=prefix, suffix=suffix)

    def setUp(self):
        self._root_dir = TestDir.mkdtemp()

        self._pushd(self._root_dir)
        self.create(self.FOO, self.FOO_CONTENTS)
        self.create(self.BAR, self.BAR_CONTENTS)
        self.create(self.CODE, self.CODE_CONTENTS)
        os.mkdir(self.DATA_DIR)
        os.mkdir(self.DATA_SUB_DIR)
        self.create(self.DATA, self.DATA_CONTENTS)
        self.create(self.DATA_SUB, self.DATA_SUB_CONTENTS)

    def tearDown(self):
        self._popd()


class TestGit(TestDir):
    N_RETRIES = 5

    def setUp(self):
        super(TestGit, self).setUp()
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


class TestGitSubmodule(TestGit):
    def setUp(self):
        super(TestGitSubmodule, self).setUp()
        subrepo = Repo.init()
        subrepo_path = "subrepo"
        self.git.create_submodule(subrepo_path, subrepo_path, subrepo.git_dir)
        self._pushd(subrepo_path)


class TestDvc(TestGit):
    def setUp(self):
        super(TestDvc, self).setUp()
        self.dvc = DvcRepo.init(self._root_dir)
        logger.be_verbose()

import logging
import os
import tempfile
import warnings
from unittest import TestCase

import pytest
import shortuuid
from git import Repo
from git.exc import GitCommandNotFound

from dvc.repo import Repo as DvcRepo
from dvc.utils.fs import remove

logger = logging.getLogger("dvc")


class TestDirFixture:
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

    def __init__(self):
        root_dir = self.mkdtemp()
        self._root_dir = os.path.realpath(root_dir)
        self._saved_dir = None

    @property
    def root_dir(self):
        return self._root_dir

    def _pushd(self, d):
        if not self._saved_dir:
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
    def mkdtemp(base_directory=None):
        prefix = f"dvc-test.{os.getpid()}."
        suffix = f".{shortuuid.uuid()}"
        return tempfile.mkdtemp(
            prefix=prefix, suffix=suffix, dir=base_directory
        )

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
            remove(self._root_dir)
        except OSError as exc:
            # pylint: disable=no-member
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
        super().setUp()
        # NOTE: handles EAGAIN error on BSD systems (osx in our case).
        # Otherwise when running tests you might get this exception:
        #
        #    GitCommandNotFound: Cmd('git') not found due to:
        #        OSError('[Errno 35] Resource temporarily unavailable')
        retries = self.N_RETRIES
        while True:
            try:
                self.git = Repo.init()
                break
            except GitCommandNotFound:
                retries -= 1
                if not retries:
                    raise

        self.git.index.add([self.CODE])
        self.git.index.commit("add code")

    def tearDown(self):
        self.git.close()
        super().tearDown()


class TestGitSubmoduleFixture(TestGitFixture):
    def setUp(self):
        super().setUp()
        subrepo = Repo.init()
        subrepo_path = "subrepo"
        self.git.create_submodule(subrepo_path, subrepo_path, subrepo.git_dir)
        self._pushd(subrepo_path)


class TestDvcFixture(TestDirFixture):
    def setUp(self):
        super().setUp()
        self.dvc = DvcRepo.init(self.root_dir, no_scm=True)


class TestDvcGitFixture(TestGitFixture):
    def setUp(self):
        super().setUp()
        self.dvc = DvcRepo.init(self.root_dir)
        self.dvc.scm.commit("init dvc")

    def tearDown(self):
        self.dvc.scm.close()
        super().tearDown()


# NOTE: Inheritance order in the classes below is important.


class TestDir(TestDirFixture, TestCase):
    def __init__(self, methodName):
        TestDirFixture.__init__(self)
        TestCase.__init__(self, methodName)


class TestGit(TestGitFixture, TestCase):
    def __init__(self, methodName):
        TestGitFixture.__init__(self)
        TestCase.__init__(self, methodName)


class TestGitSubmodule(TestGitSubmoduleFixture, TestCase):
    def __init__(self, methodName):
        TestGitSubmoduleFixture.__init__(self)
        TestCase.__init__(self, methodName)


class TestDvc(TestDvcFixture, TestCase):
    def __init__(self, methodName):
        TestDvcFixture.__init__(self)
        TestCase.__init__(self, methodName)
        self._caplog = None

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog


class TestDvcGit(TestDvcGitFixture, TestCase):
    def __init__(self, methodName):
        TestDvcGitFixture.__init__(self)
        TestCase.__init__(self, methodName)
        self._caplog = None

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog


class TestNexus3TempDir(TestDirFixture):
    """
    Temporary filesystem storage to imitate the storage
    of a Nexus 3 RAW artifactory.

    Used by the `Nexus3ClientMock` class.
    Each artefact has its own id and a stored md5 value.
    """

    file_info_storage = {}

    def __init__(self):
        self.file_info_storage = {}
        super().__init__()

    def _add_to_storage(self, name, md5, size):
        import uuid

        self.file_info_storage.update(
            {name: {"id": str(uuid.uuid4()), "md5": md5, "size": size}}
        )

    def get_info_by_file_path(self, name):
        return self.file_info_storage.get(name)

    def get_file_path_by_id(self, file_id):
        found_file_paths = [
            file_path
            for file_path, id_md5 in self.file_info_storage
            if id_md5["id"] == file_id
        ]
        if len(found_file_paths) > 0:
            return found_file_paths[0]
        return None

    def remove(self, name):
        remove(f"{self.root_dir}/{name}")
        del self.file_info_storage[name]

    def list_all_files(self):
        return self.file_info_storage.keys()

    def create(self, name, contents):
        super().create(f"{self.root_dir}/{name}", contents)
        file_size = os.path.getsize(f"{self.root_dir}/{name}")
        import hashlib

        self._add_to_storage(
            name, hashlib.md5(contents).hexdigest(), file_size
        )

    def tearDown(self):
        self.file_info_storage = {}
        self._saved_dir = self.root_dir
        super().tearDown()

"""
The goal of this module is making dvc functional tests setup a breeze. This
includes a temporary dir, initializing git and dvc repos and bootstrapping some
file structure.

The cornerstone of these fixtures is `tmp_dir`, which creates a temporary dir
and changes path to it, it might be combined with `scm` and `dvc` to initialize
empty git and dvc repos. `tmp_dir` returns a Path instance, which should save
you from using `open()`, `os` and `os.path` utils many times:

    (tmp_dir / "some_file").write_text("some text")
    # ...
    assert "some text" == (tmp_dir / "some_file").read_text()
    assert (tmp_dir / "some_file").exists()

Additionally it provides `.gen()`, `.scm_gen()` and `.dvc_gen()` methods to
bootstrap a required file structure in a single call:

    # Generate a dir with files
    tmp_dir.gen({"dir": {"file": "file text", "second_file": "..."}})

    # Generate a single file, dirs will be created along the way
    tmp_dir.gen("dir/file", "file text")

    # Generate + git add
    tmp_dir.scm_gen({"file1": "...", ...})

    # Generate + git add + git commit
    tmp_dir.scm_gen({"file1": "...", ...}, commit="add files")

    # Generate + dvc add
    tmp_dir.dvc_gen({"file1": "...", ...})

    # Generate + dvc add + git commit -am "..."
    # This commits stages to git not the generated files.
    tmp_dir.dvc_gen({"file1": "...", ...}, commit="add files")

Making it easier to bootstrap things has a supergoal of incentivizing a move
from global repo template to creating everything inplace, which:

    - makes all path references local to test, enhancing readability
    - allows using telling filenames, e.g. "git_tracked_file" instead of "foo"
    - does not create unnecessary files
"""

import os
import pathlib
from contextlib import contextmanager

import pytest
from funcy.py3 import lmap, retry

from dvc.utils import makedirs
from dvc.compat import fspath, fspath_py35


__all__ = ["tmp_dir", "scm", "dvc", "repo_template", "run_copy", "erepo_dir"]
REPO_TEMPLATE = {
    "foo": "foo",
    "bar": "bar",
    "dir": {
        "data": "dir/data text",
        "subdir": {"subdata": "dir/subdir/subdata text"},
    },
}


class TmpDir(pathlib.Path):
    def __new__(cls, *args, **kwargs):
        if cls is TmpDir:
            cls = WindowsTmpDir if os.name == "nt" else PosixTmpDir
        self = cls._from_parts(args, init=False)
        if not self._flavour.is_supported:
            raise NotImplementedError(
                "cannot instantiate %r on your system" % (cls.__name__,)
            )
        self._init()
        return self

    # Not needed in Python 3.6+
    def __fspath__(self):
        return str(self)

    def _require(self, name):
        if not hasattr(self, name):
            raise TypeError(
                "Can't use {name} for this temporary dir. "
                'Did you forget to use "{name}" fixture?'.format(name=name)
            )

    # Bootstrapping methods
    def gen(self, struct, text=""):
        if isinstance(struct, (str, bytes, pathlib.PurePath)):
            struct = {struct: text}

        self._gen(struct)
        return struct.keys()

    def _gen(self, struct, prefix=None):
        for name, contents in struct.items():
            path = (prefix or self) / name

            if isinstance(contents, dict):
                if not contents:
                    makedirs(path, exist_ok=True)
                else:
                    self._gen(contents, prefix=path)
            else:
                makedirs(path.parent, exist_ok=True)
                if isinstance(contents, bytes):
                    path.write_bytes(contents)
                else:
                    path.write_text(contents, encoding="utf-8")

    def dvc_gen(self, struct, text="", commit=None):
        paths = self.gen(struct, text)
        return self.dvc_add(paths, commit=commit)

    def scm_gen(self, struct, text="", commit=None):
        paths = self.gen(struct, text)
        return self.scm_add(paths, commit=commit)

    def dvc_add(self, filenames, commit=None):
        self._require("dvc")
        filenames = _coerce_filenames(filenames)

        stages = self.dvc.add(filenames)
        if commit:
            stage_paths = [s.path for s in stages]
            self.scm_add(stage_paths, commit=commit)

        return stages

    def scm_add(self, filenames, commit=None):
        self._require("scm")
        filenames = _coerce_filenames(filenames)

        self.scm.add(filenames)
        if commit:
            self.scm.commit(commit)

    # contexts
    @contextmanager
    def chdir(self):
        old = os.getcwd()
        try:
            os.chdir(fspath_py35(self))
            yield
        finally:
            os.chdir(old)

    @contextmanager
    def branch(self, name, new=False):
        self._require("scm")
        old = self.scm.active_branch()
        try:
            self.scm.checkout(name, create_new=new)
            yield
        finally:
            self.scm.checkout(old)

    # Introspection methods
    def list(self):
        return [p.name for p in self.iterdir()]


def _coerce_filenames(filenames):
    if isinstance(filenames, (str, bytes, pathlib.PurePath)):
        filenames = [filenames]
    return lmap(fspath, filenames)


class WindowsTmpDir(TmpDir, pathlib.PureWindowsPath):
    pass


class PosixTmpDir(TmpDir, pathlib.PurePosixPath):
    pass


@pytest.fixture
def tmp_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return TmpDir(fspath_py35(tmp_path))


@pytest.fixture
def scm(tmp_dir, request):
    # Use dvc.scm if available
    if "dvc" in request.fixturenames:
        dvc = request.getfixturevalue("dvc")
        tmp_dir.scm = dvc.scm
        yield dvc.scm

    else:
        from dvc.scm.git import Git

        _git_init()
        try:
            scm = tmp_dir.scm = Git(fspath(tmp_dir))
            yield scm
        finally:
            scm.close()


def _git_init():
    from git import Repo
    from git.exc import GitCommandNotFound

    # NOTE: handles EAGAIN error on BSD systems (osx in our case).
    # Otherwise when running tests you might get this exception:
    #
    #    GitCommandNotFound: Cmd('git') not found due to:
    #        OSError('[Errno 35] Resource temporarily unavailable')
    git = retry(5, GitCommandNotFound)(Repo.init)()
    git.close()


@pytest.fixture
def dvc(tmp_dir, request):
    from dvc.repo import Repo

    if "scm" in request.fixturenames:
        if not hasattr(tmp_dir, "scm"):
            _git_init()

        dvc = Repo.init(fspath(tmp_dir))
        dvc.scm.commit("init dvc")
    else:
        dvc = Repo.init(fspath(tmp_dir), no_scm=True)

    try:
        tmp_dir.dvc = dvc
        yield dvc
    finally:
        dvc.close()


@pytest.fixture
def repo_template(tmp_dir):
    tmp_dir.gen(REPO_TEMPLATE)


@pytest.fixture
def run_copy(tmp_dir, dvc, request):
    tmp_dir.gen(
        "copy.py",
        "import sys, shutil\nshutil.copyfile(sys.argv[1], sys.argv[2])",
    )

    # Do we need this?
    if "scm" in request.fixturenames:
        request.getfixturevalue("scm")
        tmp_dir.scm_add("copy.py", commit="add copy.py")

    def run_copy(src, dst, **run_kwargs):
        return dvc.run(
            cmd="python copy.py {} {}".format(src, dst),
            outs=[dst],
            deps=[src, "copy.py"],
            **run_kwargs
        )

    return run_copy


@pytest.fixture
def erepo_dir(tmp_path_factory, monkeypatch):
    from dvc.repo import Repo
    from dvc.remote.config import RemoteConfig

    path = TmpDir(fspath_py35(tmp_path_factory.mktemp("erepo")))

    # Chdir for git and dvc to work locally
    with path.chdir():
        _git_init()
        path.dvc = Repo.init()
        path.scm = path.dvc.scm
        path.scm.commit("init dvc")

        rconfig = RemoteConfig(path.dvc.config)
        rconfig.add("upstream", path.dvc.cache.local.cache_dir, default=True)
        path.scm_add([path.dvc.config.config_file], commit="add remote")

        path.dvc.close()

    return path

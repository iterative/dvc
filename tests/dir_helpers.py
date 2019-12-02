from __future__ import unicode_literals

import os
import pytest

from funcy.py3 import lmap, retry

from dvc.utils import makedirs
from dvc.utils.compat import basestring, is_py2, pathlib, fspath, fspath_py35


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

    def gen(self, struct, text=""):
        if isinstance(struct, basestring):
            struct = {struct: text}

        self._gen(struct)
        return struct.keys()

    def _gen(self, struct, prefix=None):
        for name, contents in struct.items():
            path = (prefix or self) / name

            if isinstance(contents, dict):
                self._gen(contents, prefix=path)
            else:
                makedirs(path.parent, exist_ok=True)
                if is_py2 and isinstance(contents, str):
                    path.write_bytes(contents)
                else:
                    path.write_text(contents)

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

    # Introspection methods
    def list(self):
        return [p.name for p in self.iterdir()]


def _coerce_filenames(filenames):
    if isinstance(filenames, (basestring, pathlib.PurePath)):
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
        tmp_dir.git_add("copy.py", commit="add copy.py")

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
    monkeypatch.chdir(fspath_py35(path))

    _git_init()
    path.dvc = Repo.init()
    path.scm = path.dvc.scm
    path.dvc_gen(REPO_TEMPLATE, commit="init repo")

    rconfig = RemoteConfig(path.dvc.config)
    rconfig.add("upstream", path.dvc.cache.local.cache_dir, default=True)
    path.scm_add([path.dvc.config.config_file], commit="add remote")

    path.dvc_gen("version", "master")
    path.scm_add([".gitignore", "version.dvc"], commit="master")

    path.scm.checkout("branch", create_new=True)
    (path / "version").unlink()  # For mac ???
    path.dvc_gen("version", "branch")
    path.scm_add([".gitignore", "version.dvc"], commit="branch")

    path.scm.checkout("master")
    path.dvc.close()
    monkeypatch.undo()  # Undo chdir

    return path

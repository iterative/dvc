"""
The goal of this module is making dvc functional tests setup a breeze. This
includes a temporary dir, initializing git and DVC repos and bootstrapping some
file structure.

The cornerstone of these fixtures is `tmp_dir`, which creates a temporary dir
and changes path to it, it might be combined with `scm` and `dvc` to initialize
empty git and DVC repos. `tmp_dir` returns a Path instance, which should save
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

# pylint: disable=redefined-outer-name, attribute-defined-outside-init

import os
import pathlib
import sys
from contextlib import contextmanager
from functools import partialmethod

from dvc.utils import serialize
from dvc.utils.fs import makedirs


class TmpDir(pathlib.Path):
    scheme = "local"

    @property
    def fs_path(self):
        return os.fspath(self)

    @property
    def url(self):
        return self.fs_path

    @property
    def config(self):
        return {"url": self.url}

    def __new__(cls, *args, **kwargs):
        if cls is TmpDir:
            cls = (  # pylint: disable=self-cls-assignment
                WindowsTmpDir if os.name == "nt" else PosixTmpDir
            )
        # init parameter and `_init` method has been removed in Python 3.10.
        kw = {"init": False} if sys.version_info < (3, 10) else {}
        self = cls._from_parts(  # pylint: disable=unexpected-keyword-arg
            args, **kw
        )
        if not self._flavour.is_supported:
            raise NotImplementedError(
                f"cannot instantiate {cls.__name__!r} on your system"
            )
        if sys.version_info < (3, 10):
            self._init()  # pylint: disable=no-member
        return self

    def init(self, *, scm=False, dvc=False, subdir=False):
        from scmrepo.git import Git

        from dvc.repo import Repo

        assert not scm or not hasattr(self, "scm")
        assert not dvc or not hasattr(self, "dvc")

        if scm:
            Git.init(self.fs_path).close()
        if dvc:
            self.dvc = Repo.init(
                self.fs_path,
                no_scm=not scm and not hasattr(self, "scm"),
                subdir=subdir,
            )
        if scm:
            self.scm = (
                self.dvc.scm if hasattr(self, "dvc") else Git(self.fs_path)
            )
        if dvc and hasattr(self, "scm"):
            self.scm.commit("init dvc")

    def close(self):
        if hasattr(self, "scm"):
            self.scm.close()
        if hasattr(self, "dvc"):
            self.dvc.close()

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

        return self._gen(struct)

    def _gen(self, struct, prefix=None):
        paths = []
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
            paths.append(path)
        return paths

    def dvc_gen(self, struct, text="", commit=None):
        paths = self.gen(struct, text)
        return self.dvc_add(paths, commit=commit)

    def scm_gen(self, struct, text="", commit=None):
        paths = self.gen(struct, text)
        return self.scm_add(paths, commit=commit)

    def commit(self, output_paths, msg):
        def to_gitignore(stage_path):
            from scmrepo.git import Git

            return os.path.join(os.path.dirname(stage_path), Git.GITIGNORE)

        gitignores = [
            to_gitignore(s)
            for s in output_paths
            if os.path.exists(to_gitignore(s))
        ]
        return self.scm_add(output_paths + gitignores, commit=msg)

    def dvc_add(self, filenames, commit=None):
        self._require("dvc")
        filenames = _coerce_filenames(filenames)

        stages = self.dvc.add(filenames)
        if commit:
            self.commit([s.path for s in stages], msg=commit)
        return stages

    def scm_add(self, filenames, commit=None):
        self._require("scm")
        filenames = _coerce_filenames(filenames)
        self.scm.add(filenames)
        if commit:
            self.scm.commit(commit)

    def add_remote(
        self, *, url=None, config=None, name="upstream", default=True
    ):
        self._require("dvc")

        assert bool(url) ^ bool(config)

        if url:
            config = {"url": url}

        with self.dvc.config.edit() as conf:
            conf["remote"][name] = config
            if default:
                conf["core"]["remote"] = name

        if hasattr(self, "scm"):
            self.scm.add(self.dvc.config.files["repo"])
            self.scm.commit(f"add '{name}' remote")

        return url or config["url"]

    # contexts
    @contextmanager
    def chdir(self):
        old = os.getcwd()
        try:
            os.chdir(self)
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

    def read_text(self, *args, **kwargs):  # pylint: disable=signature-differs
        # NOTE: on windows we'll get PermissionError instead of
        # IsADirectoryError when we try to `open` a directory, so we can't
        # rely on exception flow control
        if self.is_dir():
            return {
                path.name: path.read_text(*args, **kwargs)
                for path in self.iterdir()
            }
        return super().read_text(*args, encoding="utf-8", **kwargs)

    def hash_to_path(self, hash_):
        return str(self / hash_[0:2] / hash_[2:])

    def dump(self, *args, **kwargs):
        return serialize.DUMPERS[self.suffix](self, *args, **kwargs)

    def parse(self, *args, **kwargs):
        return serialize.LOADERS[self.suffix](self, *args, **kwargs)

    def modify(self, *args, **kwargs):
        return serialize.MODIFIERS[self.suffix](self, *args, **kwargs)

    load_yaml = partialmethod(serialize.load_yaml)
    dump_yaml = partialmethod(serialize.dump_yaml)
    load_json = partialmethod(serialize.load_json)
    dump_json = partialmethod(serialize.dump_json)
    load_toml = partialmethod(serialize.load_toml)
    dump_toml = partialmethod(serialize.dump_toml)


def _coerce_filenames(filenames):
    if isinstance(filenames, (str, bytes, pathlib.PurePath)):
        filenames = [filenames]
    return list(map(os.fspath, filenames))


class WindowsTmpDir(TmpDir, pathlib.PureWindowsPath):
    pass


class PosixTmpDir(TmpDir, pathlib.PurePosixPath):
    pass

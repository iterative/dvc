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


import os
import pathlib

import pytest

__all__ = [
    "run_copy",
    "run_head",
    "erepo_dir",
    "git_dir",
    "git_upstream",
    "git_downstream",
]


@pytest.fixture
def run_copy(tmp_dir, copy_script, dvc):
    def run_copy(src, dst, **run_kwargs):
        wdir = pathlib.Path(run_kwargs.get("wdir", "."))
        wdir = pathlib.Path("../" * len(wdir.parts))
        script_path = wdir / "copy.py"

        return dvc.run(
            cmd=f"python {script_path} {src} {dst}",
            outs=[dst],
            deps=[src, f"{script_path}"],
            **run_kwargs,
        )

    return run_copy


@pytest.fixture
def run_head(tmp_dir, head_script, dvc):
    script = os.path.abspath(tmp_dir / "head.py")

    def run(*args, **run_kwargs):
        return dvc.run(
            **{
                "cmd": "python {} {}".format(script, " ".join(args)),
                "outs": [dep + "-1" for dep in args],
                "deps": list(args),
                **run_kwargs,
            }
        )

    return run


@pytest.fixture
def erepo_dir(make_tmp_dir):
    return make_tmp_dir("erepo", scm=True, dvc=True)


@pytest.fixture
def git_dir(make_tmp_dir):
    path = make_tmp_dir("git-erepo", scm=True)
    path.scm.commit("init repo")
    return path


class GitRemote:
    def __init__(self, tmp_dir, name, url):
        self.tmp_dir = tmp_dir
        self.remote = name
        self.url = url


@pytest.fixture
def git_upstream(tmp_dir, erepo_dir, git_dir, request):
    remote = erepo_dir if "dvc" in request.fixturenames else git_dir
    url = f"file://{remote.resolve().as_posix()}"
    tmp_dir.scm.gitpython.repo.create_remote("upstream", url)
    return GitRemote(remote, "upstream", url)


@pytest.fixture
def git_downstream(tmp_dir, erepo_dir, git_dir, request):
    remote = erepo_dir if "dvc" in request.fixturenames else git_dir
    url = f"file://{tmp_dir.resolve().as_posix()}"
    remote.scm.gitpython.repo.create_remote("upstream", url)
    return GitRemote(remote, "upstream", url)

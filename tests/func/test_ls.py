import shutil
import os
import pytest

from dvc.compat import fspath
from dvc.exceptions import PathMissingError
from dvc.scm.base import CloneError
from dvc.repo import Repo

FS_STRUCTURE = {
    "README.md": "content",
    "model/script.py": "content",
    "model/train.py": "content",
    ".gitignore": "content",
}

DVC_STRUCTURE = {
    "structure.xml": "content",
    "data/subcontent/data.xml": "content",
    "data/subcontent/statistics/data.csv": "content",
    "model/people.csv": "content",
}


def match_files(files, expected_files):
    assert set(files) == set(
        map(lambda args: os.path.join(*args), expected_files)
    )


def create_dvc_pipeline(tmp_dir, dvc):
    script = os.linesep.join(
        [
            "from pathlib import Path",
            "Path({}).touch()".format(os.path.join("out", "file")),
        ]
    )
    tmp_dir.scm_gen({"script.py": script}, commit="init")
    tmp_dir.dvc_gen({"dep": "content"}, commit="init dvc")
    dvc.run(
        **{
            "command": "python script.py",
            "outs": [os.path.join("out", "file")],
            "deps": ["dep"],
            "fname": "out.dvc",
        }
    )
    tmp_dir.scm_add(["out.dvc"], commit="run")
    shutil.rmtree("out")


def test_ls_repo(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = Repo.ls(fspath(tmp_dir))
    match_files(
        files,
        (
            (".gitignore",),
            ("README.md",),
            ("structure.xml.dvc",),
            ("model",),
            ("data",),
            ("structure.xml",),
        ),
    )


def test_ls_repo_recursive(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = Repo.ls(fspath(tmp_dir), recursive=True)
    match_files(
        files,
        (
            (".gitignore",),
            ("README.md",),
            ("structure.xml.dvc",),
            ("model", "script.py"),
            ("model", "train.py"),
            ("model", "people.csv.dvc"),
            ("data", "subcontent", "data.xml.dvc"),
            ("data", "subcontent", "statistics", "data.csv.dvc"),
            ("data", "subcontent", "statistics", "data.csv"),
            ("data", "subcontent", "data.xml"),
            ("model", "people.csv"),
            ("structure.xml",),
        ),
    )


def test_ls_repo_outs_only_recursive(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = Repo.ls(fspath(tmp_dir), recursive=True, outs_only=True)
    match_files(
        files,
        (
            ("data", "subcontent", "statistics", "data.csv"),
            ("data", "subcontent", "data.xml"),
            ("model", "people.csv"),
            ("structure.xml",),
        ),
    )


def test_ls_repo_with_target_dir(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = Repo.ls(fspath(tmp_dir), target="model")
    match_files(
        files,
        (("script.py",), ("train.py",), ("people.csv",), ("people.csv.dvc",)),
    )


def test_ls_repo_with_target_dir_outs_only_empty(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")
    tmp_dir.scm_gen({"folder/.keep": "content"}, commit="add .keep")

    with pytest.raises(PathMissingError):
        Repo.ls(fspath(tmp_dir), target="folder", outs_only=True)


def test_ls_repo_with_target_subdir(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    target = os.path.join("data", "subcontent")
    files = Repo.ls(fspath(tmp_dir), target)
    match_files(files, (("data.xml",), ("data.xml.dvc",), ("statistics",)))


def test_ls_repo_with_target_subdir_outs_only(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    target = os.path.join("data", "subcontent")
    files = Repo.ls(fspath(tmp_dir), target, outs_only=True)
    match_files(files, (("data.xml",), ("statistics",)))


def test_ls_repo_with_target_subdir_outs_only_recursive(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    target = os.path.join("data", "subcontent")
    files = Repo.ls(fspath(tmp_dir), target, outs_only=True, recursive=True)
    match_files(files, (("data.xml",), ("statistics", "data.csv")))


def test_ls_repo_with_target_file_out(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    target = os.path.join("data", "subcontent", "data.xml")
    files = Repo.ls(fspath(tmp_dir), target)
    match_files(files, (("data.xml",),))


def test_ls_repo_with_file_target_fs(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    target = "README.md"
    files = Repo.ls(fspath(tmp_dir), target, recursive=True)
    match_files(files, (("README.md",),))


def test_ls_repo_with_missed_target(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    with pytest.raises(PathMissingError) as exc_info:
        Repo.ls(fspath(tmp_dir), target="missed_target")
    assert not exc_info.value.output_only


def test_ls_repo_with_missed_target_outs_only(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    with pytest.raises(PathMissingError) as exc_info:
        Repo.ls(
            fspath(tmp_dir),
            target="missed_target",
            recursive=True,
            outs_only=True,
        )
    assert exc_info.value.output_only


def test_ls_repo_with_removed_dvc_dir(tmp_dir, dvc, scm):
    create_dvc_pipeline(tmp_dir, dvc)

    files = Repo.ls(fspath(tmp_dir))
    match_files(
        files, (("script.py",), ("dep.dvc",), ("out.dvc",), ("dep",), ("out",))
    )


def test_ls_repo_with_removed_dvc_dir_recursive(tmp_dir, dvc, scm):
    create_dvc_pipeline(tmp_dir, dvc)

    files = Repo.ls(fspath(tmp_dir), recursive=True)
    match_files(
        files,
        (
            ("script.py",),
            ("dep.dvc",),
            ("out.dvc",),
            ("dep",),
            ("out", "file"),
        ),
    )


def test_ls_repo_with_removed_dvc_dir_with_target_dir(tmp_dir, dvc, scm):
    create_dvc_pipeline(tmp_dir, dvc)

    target = "out"
    files = Repo.ls(fspath(tmp_dir), target)
    match_files(files, (("file",),))


def test_ls_repo_with_removed_dvc_dir_with_target_file(tmp_dir, dvc, scm):
    create_dvc_pipeline(tmp_dir, dvc)

    target = os.path.join("out", "file")
    files = Repo.ls(fspath(tmp_dir), target)
    match_files(files, (("file",),))


def test_ls_remote_repo(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    url = "file://{}".format(erepo_dir)
    files = Repo.ls(url)
    match_files(
        files,
        (
            (".gitignore",),
            ("README.md",),
            ("structure.xml.dvc",),
            ("model",),
            ("data",),
            ("structure.xml",),
        ),
    )


def test_ls_remote_repo_recursive(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    url = "file://{}".format(erepo_dir)
    files = Repo.ls(url, recursive=True)
    match_files(
        files,
        (
            (".gitignore",),
            ("README.md",),
            ("structure.xml.dvc",),
            ("model", "script.py"),
            ("model", "train.py"),
            ("model", "people.csv.dvc"),
            ("data", "subcontent", "data.xml.dvc"),
            ("data", "subcontent", "statistics", "data.csv.dvc"),
            ("data", "subcontent", "statistics", "data.csv"),
            ("data", "subcontent", "data.xml"),
            ("model", "people.csv"),
            ("structure.xml",),
        ),
    )


def test_ls_remote_git_only_repo_recursive(git_dir):
    with git_dir.chdir():
        git_dir.scm_gen(FS_STRUCTURE, commit="init")

    url = "file://{}".format(git_dir)
    files = Repo.ls(url, recursive=True)
    match_files(
        files,
        (
            (".gitignore",),
            ("README.md",),
            ("model", "script.py"),
            ("model", "train.py"),
        ),
    )


def test_ls_remote_repo_with_target_dir(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    url = "file://{}".format(erepo_dir)
    target = "model"
    files = Repo.ls(url, target)
    match_files(
        files,
        (("script.py",), ("train.py",), ("people.csv",), ("people.csv.dvc",)),
    )


def test_ls_remote_repo_with_rev(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    rev = erepo_dir.scm.list_all_commits()[1]
    url = "file://{}".format(erepo_dir)
    files = Repo.ls(url, rev=rev)
    match_files(files, ((".gitignore",), ("README.md",), ("model",)))


def test_ls_remote_repo_with_rev_recursive(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")

    rev = erepo_dir.scm.list_all_commits()[1]
    url = "file://{}".format(erepo_dir)
    files = Repo.ls(url, rev=rev, recursive=True)
    match_files(
        files,
        (
            ("structure.xml.dvc",),
            ("model", "people.csv.dvc"),
            ("data", "subcontent", "data.xml.dvc"),
            ("data", "subcontent", "statistics", "data.csv.dvc"),
            ("data", "subcontent", "statistics", "data.csv"),
            ("data", "subcontent", "data.xml"),
            ("model", "people.csv"),
            ("structure.xml",),
        ),
    )


def test_ls_not_existed_url():
    from time import time

    dirname = "__{}_{}".format("not_existed", time())
    with pytest.raises(CloneError):
        Repo.ls(dirname, recursive=True)

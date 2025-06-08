import os
import shutil
import textwrap
from operator import itemgetter
from os.path import join

import pytest

from dvc.fs import MemoryFileSystem
from dvc.repo import Repo
from dvc.repo.ls import _ls_tree, ls_tree
from dvc.scm import CloneError

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
    left = {(f["path"], f["isout"]) for f in files}
    right = {(os.path.join(*args), isout) for (args, isout) in expected_files}
    assert left == right


def create_dvc_pipeline(tmp_dir, dvc):
    script = textwrap.dedent(
        """\
        import os, sys
        f = sys.argv[1]
        os.makedirs(os.path.dirname(f))
        open(f, "w+").close()
    """
    )
    tmp_dir.scm_gen({"script.py": script}, commit="init")
    tmp_dir.dvc_gen({"dep": "content"}, commit="init dvc")
    dvc.run(
        cmd="python script.py {}".format(os.path.join("out", "file")),
        outs=[os.path.join("out", "file")],
        deps=["dep"],
        name="touch",
    )
    tmp_dir.scm_add(["dvc.yaml", "dvc.lock"], commit="run")
    shutil.rmtree("out")


def test_ls_repo(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = Repo.ls(os.fspath(tmp_dir))
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            (("structure.xml.dvc",), False),
            (("model",), False),
            (("data",), False),
            (("structure.xml",), True),
        ),
    )


def test_ls_repo_recursive(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = Repo.ls(os.fspath(tmp_dir), recursive=True)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            (("structure.xml.dvc",), False),
            (("model", "script.py"), False),
            (("model", "train.py"), False),
            (("model", "people.csv.dvc"), False),
            (("data", "subcontent", "data.xml.dvc"), False),
            (("data", "subcontent", "statistics", "data.csv.dvc"), False),
            (("data", "subcontent", "statistics", "data.csv"), True),
            (("data", "subcontent", "statistics", ".gitignore"), False),
            (("data", "subcontent", "data.xml"), True),
            (("data", "subcontent", ".gitignore"), False),
            (("model", "people.csv"), True),
            (("model", ".gitignore"), False),
            (("structure.xml",), True),
        ),
    )


def test_ls_repo_dvc_only_recursive(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = Repo.ls(os.fspath(tmp_dir), recursive=True, dvc_only=True)
    match_files(
        files,
        (
            (("data", "subcontent", "statistics", "data.csv"), True),
            (("data", "subcontent", "data.xml"), True),
            (("model", "people.csv"), True),
            (("structure.xml",), True),
        ),
    )


def test_ls_repo_with_new_path_dir(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen({"mysub": {}}, commit="dvc")
    tmp_dir.gen({"mysub/sub": {"foo": "content"}})

    files = Repo.ls(os.fspath(tmp_dir), path="mysub/sub")
    match_files(files, ((("foo",), False),))


def test_ls_repo_with_path_dir(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = Repo.ls(os.fspath(tmp_dir), path="model")
    match_files(
        files,
        (
            (("script.py",), False),
            (("train.py",), False),
            (("people.csv",), True),
            (("people.csv.dvc",), False),
            ((".gitignore",), False),
        ),
    )


def test_ls_repo_with_path_dir_dvc_only_empty(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")
    tmp_dir.scm_gen({"folder/.keep": "content"}, commit="add .keep")
    tmp_dir.scm_gen({"empty_scm_folder/": {}}, commit="add scm empty")
    tmp_dir.dvc_gen({"empty_dvc_folder": {}}, commit="empty dvc folder")

    with pytest.raises(FileNotFoundError):
        Repo.ls(os.fspath(tmp_dir), path="not_exist_folder")

    assert Repo.ls(os.fspath(tmp_dir), path="empty_scm_folder") == []

    assert Repo.ls(os.fspath(tmp_dir), path="folder", dvc_only=True) == []

    assert Repo.ls(os.fspath(tmp_dir), path="empty_dvc_folder", dvc_only=True) == []


def test_ls_repo_with_path_subdir(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    path = os.path.join("data", "subcontent")
    files = Repo.ls(os.fspath(tmp_dir), path)
    match_files(
        files,
        (
            (("data.xml",), True),
            (("data.xml.dvc",), False),
            (("statistics",), False),
            ((".gitignore",), False),
        ),
    )


def test_ls_repo_with_path_subdir_dvc_only(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    path = os.path.join("data", "subcontent")
    files = Repo.ls(os.fspath(tmp_dir), path, dvc_only=True)
    match_files(files, ((("data.xml",), True), (("statistics",), False)))


def test_ls_repo_with_path_subdir_dvc_only_recursive(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    path = os.path.join("data", "subcontent")
    files = Repo.ls(os.fspath(tmp_dir), path, dvc_only=True, recursive=True)
    match_files(files, ((("data.xml",), True), (("statistics", "data.csv"), True)))


def test_ls_repo_with_path_file_out(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    path = os.path.join("data", "subcontent", "data.xml")
    files = Repo.ls(os.fspath(tmp_dir), path)
    match_files(files, ((("data.xml",), True),))


def test_ls_repo_with_file_path_fs(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    path = "README.md"
    files = Repo.ls(os.fspath(tmp_dir), path, recursive=True)
    match_files(files, ((("README.md",), False),))


def test_ls_repo_with_missed_path(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    with pytest.raises(FileNotFoundError):
        Repo.ls(os.fspath(tmp_dir), path="missed_path")


def test_ls_repo_with_missed_path_dvc_only(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    with pytest.raises(FileNotFoundError):
        Repo.ls(
            os.fspath(tmp_dir),
            path="missed_path",
            recursive=True,
            dvc_only=True,
        )


def test_ls_repo_with_removed_dvc_dir(tmp_dir, dvc, scm):
    create_dvc_pipeline(tmp_dir, dvc)

    files = Repo.ls(os.fspath(tmp_dir))
    match_files(
        files,
        (
            (("script.py",), False),
            (("dep.dvc",), False),
            (("dvc.yaml",), False),
            (("dvc.lock",), False),
            (("dep",), True),
            (("out",), False),
            ((".dvcignore",), False),
            ((".gitignore",), False),
        ),
    )


def test_ls_repo_with_removed_dvc_dir_recursive(tmp_dir, dvc, scm):
    create_dvc_pipeline(tmp_dir, dvc)

    files = Repo.ls(os.fspath(tmp_dir), recursive=True)
    match_files(
        files,
        (
            (("script.py",), False),
            (("dep.dvc",), False),
            (("dvc.yaml",), False),
            (("dvc.lock",), False),
            (("dep",), True),
            (("out", "file"), True),
            ((".dvcignore",), False),
            ((".gitignore",), False),
        ),
    )


def test_ls_repo_with_removed_dvc_dir_with_path_dir(tmp_dir, dvc, scm):
    create_dvc_pipeline(tmp_dir, dvc)

    path = "out"
    files = Repo.ls(os.fspath(tmp_dir), path)
    match_files(files, ((("file",), True),))


def test_ls_repo_with_removed_dvc_dir_with_path_file(tmp_dir, dvc, scm):
    create_dvc_pipeline(tmp_dir, dvc)

    path = os.path.join("out", "file")
    files = Repo.ls(os.fspath(tmp_dir), path)
    match_files(files, ((("file",), True),))


def test_ls_repo_with_rev(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    rev = erepo_dir.scm.list_all_commits()[1]
    files = Repo.ls(os.fspath(erepo_dir), rev=rev)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            (("model",), False),
        ),
    )


def test_ls_remote_repo(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    url = f"file://{erepo_dir.as_posix()}"
    files = Repo.ls(url)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            (("structure.xml.dvc",), False),
            (("model",), False),
            (("data",), False),
            (("structure.xml",), True),
        ),
    )


def test_ls_remote_repo_recursive(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    url = f"file://{erepo_dir.as_posix()}"
    files = Repo.ls(url, recursive=True)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            (("structure.xml.dvc",), False),
            (("model", "script.py"), False),
            (("model", "train.py"), False),
            (("model", "people.csv.dvc"), False),
            (("data", "subcontent", "data.xml.dvc"), False),
            (("data", "subcontent", "statistics", "data.csv.dvc"), False),
            (("data", "subcontent", "statistics", "data.csv"), True),
            (("data", "subcontent", "statistics", ".gitignore"), False),
            (("data", "subcontent", "data.xml"), True),
            (("data", "subcontent", ".gitignore"), False),
            (("model", "people.csv"), True),
            (("model", ".gitignore"), False),
            (("structure.xml",), True),
        ),
    )


def test_ls_remote_git_only_repo_recursive(git_dir):
    with git_dir.chdir():
        git_dir.scm_gen(FS_STRUCTURE, commit="init")

    url = f"file://{git_dir.as_posix()}"
    files = Repo.ls(url, recursive=True)
    match_files(
        files,
        (
            ((".gitignore",), False),
            (("README.md",), False),
            (("model", "script.py"), False),
            (("model", "train.py"), False),
        ),
    )


def test_ls_remote_repo_with_path_dir(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    url = f"file://{erepo_dir.as_posix()}"
    path = "model"
    files = Repo.ls(url, path)
    match_files(
        files,
        (
            (("script.py",), False),
            (("train.py",), False),
            (("people.csv",), True),
            (("people.csv.dvc",), False),
            ((".gitignore",), False),
        ),
    )


def test_ls_remote_repo_with_rev(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    rev = erepo_dir.scm.list_all_commits()[1]
    url = f"file://{erepo_dir.as_posix()}"
    files = Repo.ls(url, rev=rev)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            (("model",), False),
        ),
    )


def test_ls_remote_repo_with_rev_recursive(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")

    rev = erepo_dir.scm.list_all_commits()[1]
    url = f"file://{erepo_dir.as_posix()}"
    files = Repo.ls(url, rev=rev, recursive=True)
    match_files(
        files,
        (
            (("structure.xml.dvc",), False),
            (("model", "people.csv.dvc"), False),
            (("data", "subcontent", "data.xml.dvc"), False),
            (("data", "subcontent", "statistics", "data.csv.dvc"), False),
            (("data", "subcontent", "statistics", "data.csv"), True),
            (("data", "subcontent", "statistics", ".gitignore"), False),
            (("data", "subcontent", "data.xml"), True),
            (("data", "subcontent", ".gitignore"), False),
            (("model", "people.csv"), True),
            (("model", ".gitignore"), False),
            (("structure.xml",), True),
            ((".dvcignore",), False),
            ((".gitignore",), False),
        ),
    )


def test_ls_not_existed_url():
    from time import time

    dirname = "__{}_{}".format("not_existed", time())
    with pytest.raises(CloneError):
        Repo.ls(dirname, recursive=True)


def test_ls_shows_pipeline_tracked_outs(tmp_dir, dvc, scm, run_copy):
    from dvc.dvcfile import LOCK_FILE, PROJECT_FILE

    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    dvc.scm.add([PROJECT_FILE, LOCK_FILE])
    dvc.scm.commit("add pipeline stage")

    files = Repo.ls(os.curdir, dvc_only=True)
    match_files(files, ((("bar",), True),))


def test_ls_granular(erepo_dir, M):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen(
            {
                "dir": {
                    "1": "1",
                    "2": "2",
                    "subdir": {"foo": "foo", "bar": "bar"},
                }
            },
            commit="create dir",
        )

    entries = Repo.ls(os.fspath(erepo_dir), os.path.join("dir", "subdir"))
    assert entries == [
        {
            "isout": True,
            "isdir": False,
            "isexec": False,
            "path": "bar",
            "size": 3,
            "md5": "37b51d194a7513e45b56f6524f2d51f2",
        },
        {
            "isout": True,
            "isdir": False,
            "isexec": False,
            "path": "foo",
            "size": 3,
            "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
        },
    ]

    entries = Repo.ls(os.fspath(erepo_dir), "dir")
    assert entries == [
        {
            "isout": True,
            "isdir": False,
            "isexec": False,
            "path": "1",
            "size": 1,
            "md5": "c4ca4238a0b923820dcc509a6f75849b",
        },
        {
            "isout": True,
            "isdir": False,
            "isexec": False,
            "path": "2",
            "size": 1,
            "md5": "c81e728d9d4c2f636f067f89cc14862c",
        },
        {
            "isout": True,
            "isdir": True,
            "isexec": False,
            "path": "subdir",
            "size": M.instance_of(int),
            "md5": None,
        },
    ]


@pytest.mark.parametrize("use_scm", [True, False])
def test_ls_target(erepo_dir, use_scm):
    with erepo_dir.chdir():
        gen = erepo_dir.scm_gen if use_scm else erepo_dir.dvc_gen
        gen(
            {
                "dir": {
                    "1": "1",
                    "2": "2",
                    "subdir": {"foo": "foo", "bar": "bar"},
                }
            },
            commit="create dir",
        )

    isout = not use_scm

    def _ls(path):
        return Repo.ls(os.fspath(erepo_dir), path)

    assert _ls(os.path.join("dir", "1")) == [
        {
            "isout": isout,
            "isdir": False,
            "isexec": False,
            "path": "1",
            "size": 1,
            "md5": "c4ca4238a0b923820dcc509a6f75849b" if not use_scm else None,
        }
    ]
    assert _ls(os.path.join("dir", "subdir", "foo")) == [
        {
            "isout": isout,
            "isdir": False,
            "isexec": False,
            "path": "foo",
            "size": 3,
            "md5": "acbd18db4cc2f85cedef654fccc4a4d8" if not use_scm else None,
        }
    ]
    assert _ls(os.path.join("dir", "subdir")) == [
        {
            "isdir": False,
            "isexec": 0,
            "isout": isout,
            "path": "bar",
            "size": 3,
            "md5": "37b51d194a7513e45b56f6524f2d51f2" if not use_scm else None,
        },
        {
            "isdir": False,
            "isexec": 0,
            "isout": isout,
            "path": "foo",
            "size": 3,
            "md5": "acbd18db4cc2f85cedef654fccc4a4d8" if not use_scm else None,
        },
    ]


@pytest.mark.parametrize(
    "dvc_top_level, erepo_type",
    [
        (True, "erepo_dir"),
        (False, "git_dir"),
    ],
)
def test_subrepo(request, dvc_top_level, erepo_type):
    from tests.func.test_get import make_subrepo

    dvc_files = {"foo.txt": "foo.txt", "dvc_dir": {"lorem": "lorem"}}
    scm_files = {"bar.txt": "bar.txt", "scm_dir": {"ipsum": "ipsum"}}

    erepo = request.getfixturevalue(erepo_type)
    subrepo = erepo / "subrepo"
    make_subrepo(subrepo, erepo.scm)

    for repo in [erepo, subrepo]:
        with repo.chdir():
            repo.scm_gen(scm_files, commit=f"scm track for top {repo}")
            if hasattr(repo, "dvc"):
                repo.dvc_gen(dvc_files, commit=f"dvc track for {repo}")

    def _list_files(repo, path=None):
        return set(map(itemgetter("path"), Repo.ls(os.fspath(repo), path)))

    extras = {".dvcignore", ".gitignore"}
    git_tracked_outputs = {"bar.txt", "scm_dir"}
    dvc_files = {"dvc_dir", "foo.txt", "foo.txt.dvc", "dvc_dir.dvc"}
    common_outputs = git_tracked_outputs | extras | dvc_files

    top_level_outputs = common_outputs if dvc_top_level else git_tracked_outputs
    assert _list_files(erepo) == top_level_outputs
    assert _list_files(erepo, "scm_dir") == {"ipsum"}
    if dvc_top_level:
        assert _list_files(erepo, "dvc_dir") == {"lorem"}

    assert _list_files(subrepo, ".") == common_outputs
    assert _list_files(subrepo, "scm_dir") == {"ipsum"}
    assert _list_files(subrepo, "dvc_dir") == {"lorem"}


def test_broken_symlink(tmp_dir, dvc, M):
    from dvc.fs import system

    tmp_dir.gen("file", "content")
    system.symlink("file", "link")

    os.remove("file")

    entries = Repo.ls(os.fspath(tmp_dir))

    assert entries == [
        {
            "isout": False,
            "isdir": False,
            "isexec": False,
            "path": ".dvcignore",
            "size": M.instance_of(int),
            "md5": None,
        },
        {
            "isout": False,
            "isdir": False,
            "isexec": False,
            "path": "link",
            "size": 0,
            "md5": None,
        },
    ]


def test_ls_broken_dir(tmp_dir, dvc, M):
    from dvc_data.index import DataIndexDirError

    tmp_dir.dvc_gen(
        {
            "broken": {"baz": "baz"},
        }
    )

    shutil.rmtree(tmp_dir / "broken")
    dvc.cache.local.clear()

    tmp_dir.dvc_gen(
        {
            "foo": "foo",
            "dir": {"bar": "bar"},
        }
    )

    entries = Repo.ls(os.fspath(tmp_dir))
    assert entries == [
        {
            "isdir": False,
            "isexec": False,
            "isout": False,
            "path": ".dvcignore",
            "size": M.instance_of(int),
            "md5": None,
        },
        {
            "isdir": True,
            "isexec": False,
            "isout": True,
            "path": "broken",
            "size": 3,
            "md5": "630bd47b538d2a513c7d267d07e0bc44.dir",
        },
        {
            "isdir": False,
            "isexec": False,
            "isout": False,
            "path": "broken.dvc",
            "size": M.instance_of(int),
            "md5": None,
        },
        {
            "isdir": True,
            "isexec": False,
            "isout": True,
            "path": "dir",
            "size": M.instance_of(int),
            "md5": "91aaa9bb58b657d623ef143b195a67e4.dir",
        },
        {
            "isdir": False,
            "isexec": False,
            "isout": False,
            "path": "dir.dvc",
            "size": M.instance_of(int),
            "md5": None,
        },
        {
            "isdir": False,
            "isexec": False,
            "isout": True,
            "path": "foo",
            "size": 3,
            "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
        },
        {
            "isdir": False,
            "isexec": False,
            "isout": False,
            "path": "foo.dvc",
            "size": M.instance_of(int),
            "md5": None,
        },
    ]

    with pytest.raises(DataIndexDirError):
        Repo.ls(os.fspath(tmp_dir), "broken")

    with pytest.raises(DataIndexDirError):
        Repo.ls(os.fspath(tmp_dir), recursive=True)


def test_ls_maxdepth(tmp_dir, scm, dvc):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = Repo.ls(os.fspath(tmp_dir), "structure.xml", maxdepth=0, recursive=True)
    match_files(files, ((("structure.xml",), True),))

    files = Repo.ls(os.fspath(tmp_dir), maxdepth=0, recursive=True)
    match_files(files, (((os.curdir,), False),))

    files = Repo.ls(os.fspath(tmp_dir), maxdepth=1, recursive=True)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            (("structure.xml.dvc",), False),
            (("model",), False),
            (("data",), False),
            (("structure.xml",), True),
        ),
    )
    files = Repo.ls(os.fspath(tmp_dir), maxdepth=2, recursive=True)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            ((join("data", "subcontent"),), False),
            ((join("model", ".gitignore"),), False),
            ((join("model", "people.csv"),), True),
            ((join("model", "people.csv.dvc"),), False),
            ((join("model", "script.py"),), False),
            ((join("model", "train.py"),), False),
            (("structure.xml",), True),
            (("structure.xml.dvc",), False),
        ),
    )

    files = Repo.ls(os.fspath(tmp_dir), maxdepth=3, recursive=True)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            ((join("data", "subcontent", ".gitignore"),), False),
            ((join("data", "subcontent", "data.xml"),), True),
            ((join("data", "subcontent", "data.xml.dvc"),), False),
            ((join("data", "subcontent", "statistics"),), False),
            ((join("model", ".gitignore"),), False),
            ((join("model", "people.csv"),), True),
            ((join("model", "people.csv.dvc"),), False),
            ((join("model", "script.py"),), False),
            ((join("model", "train.py"),), False),
            ((join("structure.xml"),), True),
            ((join("structure.xml.dvc"),), False),
        ),
    )

    files = Repo.ls(os.fspath(tmp_dir), maxdepth=4, recursive=True)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            ((join("data", "subcontent", ".gitignore"),), False),
            ((join("data", "subcontent", "data.xml"),), True),
            ((join("data", "subcontent", "data.xml.dvc"),), False),
            ((join("data", "subcontent", "statistics", ".gitignore"),), False),
            ((join("data", "subcontent", "statistics", "data.csv"),), True),
            ((join("data", "subcontent", "statistics", "data.csv.dvc"),), False),
            ((join("model", ".gitignore"),), False),
            ((join("model", "people.csv"),), True),
            ((join("model", "people.csv.dvc"),), False),
            ((join("model", "script.py"),), False),
            ((join("model", "train.py"),), False),
            (("structure.xml",), True),
            (("structure.xml.dvc",), False),
        ),
    )


def _simplify_tree(files):
    ret = {}
    for path, info in files.items():
        if content := info.get("contents"):
            ret[path] = _simplify_tree(content)
        else:
            ret[path] = None
    return ret


def test_ls_tree(M, tmp_dir, scm, dvc):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = ls_tree(os.fspath(tmp_dir), "structure.xml")
    assert _simplify_tree(files) == {"structure.xml": None}

    files = ls_tree(os.fspath(tmp_dir))

    expected = {
        ".": {
            ".dvcignore": None,
            ".gitignore": None,
            "README.md": None,
            "data": {
                "subcontent": {
                    ".gitignore": None,
                    "data.xml": None,
                    "data.xml.dvc": None,
                    "statistics": {
                        ".gitignore": None,
                        "data.csv": None,
                        "data.csv.dvc": None,
                    },
                }
            },
            "model": {
                ".gitignore": None,
                "people.csv": None,
                "people.csv.dvc": None,
                "script.py": None,
                "train.py": None,
            },
            "structure.xml": None,
            "structure.xml.dvc": None,
        }
    }
    assert _simplify_tree(files) == expected

    files = ls_tree(os.fspath(tmp_dir), "model")
    assert _simplify_tree(files) == {
        "model": {
            ".gitignore": None,
            "people.csv": None,
            "people.csv.dvc": None,
            "script.py": None,
            "train.py": None,
        }
    }


def test_ls_tree_dvc_only(M, tmp_dir, scm, dvc):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = ls_tree(os.fspath(tmp_dir), dvc_only=True)

    expected = {
        ".": {
            "data": {
                "subcontent": {"data.xml": None, "statistics": {"data.csv": None}}
            },
            "model": {"people.csv": None},
            "structure.xml": None,
        }
    }
    assert _simplify_tree(files) == expected


def test_ls_tree_maxdepth(M, tmp_dir, scm, dvc):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = ls_tree(os.fspath(tmp_dir), maxdepth=0)
    assert _simplify_tree(files) == {".": None}

    files = ls_tree(os.fspath(tmp_dir), maxdepth=1)
    assert _simplify_tree(files) == {
        ".": {
            ".dvcignore": None,
            ".gitignore": None,
            "README.md": None,
            "data": None,
            "model": None,
            "structure.xml": None,
            "structure.xml.dvc": None,
        }
    }

    files = ls_tree(os.fspath(tmp_dir), maxdepth=2)
    assert _simplify_tree(files) == {
        ".": {
            ".dvcignore": None,
            ".gitignore": None,
            "README.md": None,
            "data": {"subcontent": None},
            "model": {
                ".gitignore": None,
                "people.csv": None,
                "people.csv.dvc": None,
                "script.py": None,
                "train.py": None,
            },
            "structure.xml": None,
            "structure.xml.dvc": None,
        }
    }

    files = ls_tree(os.fspath(tmp_dir), maxdepth=3)
    assert _simplify_tree(files) == {
        ".": {
            ".dvcignore": None,
            ".gitignore": None,
            "README.md": None,
            "data": {
                "subcontent": {
                    ".gitignore": None,
                    "data.xml": None,
                    "data.xml.dvc": None,
                    "statistics": None,
                }
            },
            "model": {
                ".gitignore": None,
                "people.csv": None,
                "people.csv.dvc": None,
                "script.py": None,
                "train.py": None,
            },
            "structure.xml": None,
            "structure.xml.dvc": None,
        }
    }

    files = ls_tree(os.fspath(tmp_dir), maxdepth=4)
    assert _simplify_tree(files) == {
        ".": {
            ".dvcignore": None,
            ".gitignore": None,
            "README.md": None,
            "data": {
                "subcontent": {
                    ".gitignore": None,
                    "data.xml": None,
                    "data.xml.dvc": None,
                    "statistics": {
                        ".gitignore": None,
                        "data.csv": None,
                        "data.csv.dvc": None,
                    },
                }
            },
            "model": {
                ".gitignore": None,
                "people.csv": None,
                "people.csv.dvc": None,
                "script.py": None,
                "train.py": None,
            },
            "structure.xml": None,
            "structure.xml.dvc": None,
        }
    }


def test_fs_ls_tree():
    fs = MemoryFileSystem(global_store=False)
    fs.pipe({f: content.encode() for f, content in FS_STRUCTURE.items()})
    root = fs.root_marker

    files = _ls_tree(fs, "README.md")
    assert _simplify_tree(files) == {"README.md": None}
    files = _ls_tree(fs, root)
    expected = {
        root: {
            ".gitignore": None,
            "README.md": None,
            "model": {
                "script.py": None,
                "train.py": None,
            },
        }
    }
    assert _simplify_tree(files) == expected

    files = _ls_tree(fs, "model")
    assert _simplify_tree(files) == {
        "model": {
            "script.py": None,
            "train.py": None,
        }
    }


def test_fs_ls_tree_maxdepth():
    fs = MemoryFileSystem(global_store=False)
    fs.pipe({f: content.encode() for f, content in FS_STRUCTURE.items()})

    files = _ls_tree(fs, "/", maxdepth=0)
    assert _simplify_tree(files) == {"/": None}

    files = _ls_tree(fs, "/", maxdepth=1)
    assert _simplify_tree(files) == {
        "/": {
            ".gitignore": None,
            "README.md": None,
            "model": None,
        }
    }

    files = _ls_tree(fs, "/", maxdepth=2)
    assert _simplify_tree(files) == {
        "/": {
            ".gitignore": None,
            "README.md": None,
            "model": {
                "script.py": None,
                "train.py": None,
            },
        }
    }

    files = _ls_tree(fs, "README.md", maxdepth=3)
    assert _simplify_tree(files) == {"README.md": None}

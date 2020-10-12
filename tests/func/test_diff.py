import hashlib
import os

import pytest
from funcy import first

from dvc.exceptions import DvcException
from dvc.utils.fs import remove


def digest(text):
    return hashlib.md5(bytes(text, "utf-8")).hexdigest()


def test_no_scm(tmp_dir, dvc):
    from dvc.scm.base import NoSCMError

    tmp_dir.dvc_gen("file", "text")

    with pytest.raises(NoSCMError):
        dvc.diff()


def test_added(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "text")

    assert dvc.diff() == {
        "added": [{"path": "file", "hash": digest("text")}],
        "deleted": [],
        "modified": [],
        "not in cache": [],
    }


def test_no_cache_entry(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "first", commit="add a file")

    tmp_dir.dvc_gen({"dir": {"1": "1", "2": "2"}})
    tmp_dir.dvc_gen("file", "second")

    remove(tmp_dir / ".dvc" / "cache")
    (tmp_dir / ".dvc" / "tmp" / "state").unlink()

    dir_checksum = "5fb6b29836c388e093ca0715c872fe2a.dir"

    assert dvc.diff() == {
        "added": [
            {"path": os.path.join("dir", ""), "hash": dir_checksum},
            {"path": os.path.join("dir", "1"), "hash": digest("1")},
            {"path": os.path.join("dir", "2"), "hash": digest("2")},
        ],
        "deleted": [],
        "modified": [
            {
                "path": "file",
                "hash": {"old": digest("first"), "new": digest("second")},
            }
        ],
        "not in cache": [],
    }


def test_deleted(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "text", commit="add file")
    (tmp_dir / "file.dvc").unlink()

    assert dvc.diff() == {
        "added": [],
        "deleted": [{"path": "file", "hash": digest("text")}],
        "modified": [],
        "not in cache": [],
    }


def test_modified(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "first", commit="first version")
    tmp_dir.dvc_gen("file", "second")

    assert dvc.diff() == {
        "added": [],
        "deleted": [],
        "modified": [
            {
                "path": "file",
                "hash": {"old": digest("first"), "new": digest("second")},
            }
        ],
        "not in cache": [],
    }


def test_refs(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "first", commit="first version")
    tmp_dir.dvc_gen("file", "second", commit="second version")
    tmp_dir.dvc_gen("file", "third", commit="third version")

    HEAD_2 = digest("first")
    HEAD_1 = digest("second")
    HEAD = digest("third")

    assert dvc.diff("HEAD~1") == {
        "added": [],
        "deleted": [],
        "modified": [{"path": "file", "hash": {"old": HEAD_1, "new": HEAD}}],
        "not in cache": [],
    }

    assert dvc.diff("HEAD~2", "HEAD~1") == {
        "added": [],
        "deleted": [],
        "modified": [{"path": "file", "hash": {"old": HEAD_2, "new": HEAD_1}}],
        "not in cache": [],
    }

    with pytest.raises(DvcException, match=r"unknown Git revision 'missing'"):
        dvc.diff("missing")


def test_directories(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"dir": {"1": "1", "2": "2"}}, commit="add a directory")
    tmp_dir.dvc_gen({"dir": {"3": "3"}}, commit="add a file")
    tmp_dir.dvc_gen({"dir": {"2": "two"}}, commit="modify a file")

    (tmp_dir / "dir" / "2").unlink()
    dvc.add("dir")
    scm.add(["dir.dvc"])
    scm.commit("delete a file")

    # The ":/<text>" format is a way to specify revisions by commit message:
    #       https://git-scm.com/docs/revisions
    #
    assert dvc.diff(":/init", ":/directory") == {
        "added": [
            {
                "path": os.path.join("dir", ""),
                "hash": "5fb6b29836c388e093ca0715c872fe2a.dir",
            },
            {"path": os.path.join("dir", "1"), "hash": digest("1")},
            {"path": os.path.join("dir", "2"), "hash": digest("2")},
        ],
        "deleted": [],
        "modified": [],
        "not in cache": [],
    }

    assert dvc.diff(":/directory", ":/modify") == {
        "added": [{"path": os.path.join("dir", "3"), "hash": digest("3")}],
        "deleted": [],
        "modified": [
            {
                "path": os.path.join("dir", ""),
                "hash": {
                    "old": "5fb6b29836c388e093ca0715c872fe2a.dir",
                    "new": "9b5faf37366b3370fd98e3e60ca439c1.dir",
                },
            },
            {
                "path": os.path.join("dir", "2"),
                "hash": {"old": digest("2"), "new": digest("two")},
            },
        ],
        "not in cache": [],
    }

    assert dvc.diff(":/modify", ":/delete") == {
        "added": [],
        "deleted": [
            {"path": os.path.join("dir", "2"), "hash": digest("two")},
        ],
        "modified": [
            {
                "path": os.path.join("dir", ""),
                "hash": {
                    "old": "9b5faf37366b3370fd98e3e60ca439c1.dir",
                    "new": "83ae82fb367ac9926455870773ff09e6.dir",
                },
            }
        ],
        "not in cache": [],
    }


def test_diff_no_cache(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"dir": {"file": "file content"}}, commit="first")
    scm.tag("v1")

    tmp_dir.dvc_gen(
        {"dir": {"file": "modified file content"}}, commit="second"
    )
    scm.tag("v2")

    remove(dvc.cache.local.cache_dir)

    # invalidate_dir_info to force cache loading
    dvc.cache.local._dir_info = {}

    diff = dvc.diff("v1", "v2")
    assert diff["added"] == []
    assert diff["deleted"] == []
    assert first(diff["modified"])["path"] == os.path.join("dir", "")
    assert diff["not in cache"] == []

    (tmp_dir / "dir" / "file").unlink()
    remove(str(tmp_dir / "dir"))
    diff = dvc.diff()
    assert diff["added"] == []
    assert diff["deleted"] == []
    assert diff["modified"] == []
    assert diff["not in cache"] == [
        {
            "path": os.path.join("dir", ""),
            "hash": "f0f7a307d223921557c929f944bf5303.dir",
        },
    ]


def test_diff_dirty(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen(
        {"file": "file_content", "dir": {"dir_file1": "dir file content"}},
        commit="initial",
    )

    (tmp_dir / "file").unlink()
    tmp_dir.gen({"dir": {"dir_file2": "dir file 2 content"}})
    tmp_dir.dvc_gen("new_file", "new_file_content")

    result = dvc.diff()

    assert result == {
        "added": [
            {
                "hash": digest("dir file 2 content"),
                "path": os.path.join("dir", "dir_file2"),
            },
            {"hash": "86d049de17c76ac44cdcac146042ec9b", "path": "new_file"},
        ],
        "deleted": [
            {"hash": "7f0b6bb0b7e951b7fd2b2a4a326297e1", "path": "file"}
        ],
        "modified": [
            {
                "hash": {
                    "new": "38175ad60f0e58ac94e0e2b7688afd81.dir",
                    "old": "92daf39af116ca2fb245acaeb2ae65f7.dir",
                },
                "path": os.path.join("dir", ""),
            }
        ],
        "not in cache": [],
    }


def test_no_changes(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "first", commit="add a file")
    assert dvc.diff() == {}


def test_no_commits(tmp_dir):
    from dvc.repo import Repo
    from dvc.scm.git import Git
    from tests.dir_helpers import git_init

    git_init(".")
    assert Git().no_commits

    assert Repo.init().diff() == {}

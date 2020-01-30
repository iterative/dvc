import hashlib
import os
import pytest
import shutil

from dvc.compat import fspath
from dvc.exceptions import DvcException


def digest(text):
    return hashlib.md5(bytes(text, "utf-8")).hexdigest()


def test_no_scm(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "text")

    with pytest.raises(DvcException, match=r"only supported for Git repos"):
        dvc.diff()


def test_added(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "text")

    assert dvc.diff() == {
        "added": [{"path": "file", "checksum": digest("text")}],
        "deleted": [],
        "modified": [],
    }


def test_no_cache_entry(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "first", commit="add a file")

    tmp_dir.dvc_gen({"dir": {"1": "1", "2": "2"}})
    tmp_dir.dvc_gen("file", "second")

    shutil.rmtree(fspath(tmp_dir / ".dvc" / "cache"))
    (tmp_dir / ".dvc" / "state").unlink()

    dir_checksum = "5fb6b29836c388e093ca0715c872fe2a.dir"

    assert dvc.diff() == {
        "added": [
            {"path": os.path.join("dir", ""), "checksum": dir_checksum},
            {"path": os.path.join("dir", "1"), "checksum": digest("1")},
            {"path": os.path.join("dir", "2"), "checksum": digest("2")},
        ],
        "deleted": [],
        "modified": [
            {
                "path": "file",
                "checksum": {"old": digest("first"), "new": digest("second")},
            }
        ],
    }


def test_deleted(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "text", commit="add file")
    (tmp_dir / "file.dvc").unlink()

    assert dvc.diff() == {
        "added": [],
        "deleted": [{"path": "file", "checksum": digest("text")}],
        "modified": [],
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
                "checksum": {"old": digest("first"), "new": digest("second")},
            }
        ],
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
        "modified": [
            {"path": "file", "checksum": {"old": HEAD_1, "new": HEAD}}
        ],
    }

    assert dvc.diff("HEAD~2", "HEAD~1") == {
        "added": [],
        "deleted": [],
        "modified": [
            {"path": "file", "checksum": {"old": HEAD_2, "new": HEAD_1}}
        ],
    }

    with pytest.raises(DvcException, match=r"unknown Git revision 'missing'"):
        dvc.diff("missing")


def test_directories(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"dir": {"1": "1", "2": "2"}}, commit="add a directory")
    tmp_dir.dvc_gen({"dir": {"3": "3"}}, commit="add a file")
    tmp_dir.dvc_gen({"dir": {"2": "two"}}, commit="modify a file")

    (tmp_dir / "dir" / "2").unlink()
    dvc.add("dir")
    scm.add("dir.dvc")
    scm.commit("delete a file")

    # The ":/<text>" format is a way to specify revisions by commit message:
    #       https://git-scm.com/docs/revisions
    #
    assert dvc.diff(":/init", ":/directory") == {
        "added": [
            {
                "path": os.path.join("dir", ""),
                "checksum": "5fb6b29836c388e093ca0715c872fe2a.dir",
            },
            {"path": os.path.join("dir", "1"), "checksum": digest("1")},
            {"path": os.path.join("dir", "2"), "checksum": digest("2")},
        ],
        "deleted": [],
        "modified": [],
    }

    assert dvc.diff(":/directory", ":/modify") == {
        "added": [{"path": os.path.join("dir", "3"), "checksum": digest("3")}],
        "deleted": [],
        "modified": [
            {
                "path": os.path.join("dir", ""),
                "checksum": {
                    "old": "5fb6b29836c388e093ca0715c872fe2a.dir",
                    "new": "9b5faf37366b3370fd98e3e60ca439c1.dir",
                },
            },
            {
                "path": os.path.join("dir", "2"),
                "checksum": {"old": digest("2"), "new": digest("two")},
            },
        ],
    }

    assert dvc.diff(":/modify", ":/delete") == {
        "added": [],
        "deleted": [
            {"path": os.path.join("dir", "2"), "checksum": digest("two")}
        ],
        "modified": [
            {
                "path": os.path.join("dir", ""),
                "checksum": {
                    "old": "9b5faf37366b3370fd98e3e60ca439c1.dir",
                    "new": "83ae82fb367ac9926455870773ff09e6.dir",
                },
            }
        ],
    }

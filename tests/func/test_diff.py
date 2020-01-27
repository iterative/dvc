import hashlib
import json
import os
import pytest
import shutil

import colorama

from dvc.compat import fspath
from dvc.exceptions import DvcException
from dvc.main import main


def digest(text):
    return hashlib.md5(bytes(text, "utf-8")).hexdigest()


def test_no_scm(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "text")

    with pytest.raises(DvcException, match=r"only supported for Git repos"):
        dvc.diff()


def test_added(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "text")

    assert dvc.diff() == {
        "added": [{"filename": "file", "checksum": digest("text")}],
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
            {"filename": "dir/", "checksum": dir_checksum},
            {"filename": "dir/1", "checksum": digest("1")},
            {"filename": "dir/2", "checksum": digest("2")},
        ],
        "deleted": [],
        "modified": [
            {
                "filename": "file",
                "checksum": {"old": digest("first"), "new": digest("second")},
            }
        ],
    }


def test_deleted(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "text", commit="add file")
    (tmp_dir / "file.dvc").unlink()

    assert dvc.diff() == {
        "added": [],
        "deleted": [{"filename": "file", "checksum": digest("text")}],
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
                "filename": "file",
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
            {"filename": "file", "checksum": {"old": HEAD_1, "new": HEAD}}
        ],
    }

    assert dvc.diff("HEAD~2", "HEAD~1") == {
        "added": [],
        "deleted": [],
        "modified": [
            {"filename": "file", "checksum": {"old": HEAD_2, "new": HEAD_1}}
        ],
    }

    with pytest.raises(
        DvcException, match=r"failed to access revision 'missing'"
    ):
        dvc.diff("missing")


def test_target(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    tmp_dir.dvc_gen("bar", "bar")
    scm.add([".gitignore", "foo.dvc", "bar.dvc"])
    scm.commit("lowercase")

    tmp_dir.dvc_gen("foo", "FOO")
    tmp_dir.dvc_gen("bar", "BAR")
    scm.add(["foo.dvc", "bar.dvc"])
    scm.commit("uppercase")

    assert dvc.diff("HEAD~1", target="foo") == {
        "added": [],
        "deleted": [],
        "modified": [
            {
                "filename": "foo",
                "checksum": {"old": digest("foo"), "new": digest("FOO")},
            }
        ],
    }

    assert dvc.diff("HEAD~1", target="missing") == {
        "added": [],
        "deleted": [],
        "modified": [],
    }


def test_directories(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"dir": {"1": "1", "2": "2"}}, commit="add a directory")
    tmp_dir.dvc_gen({"dir": {"3": "3"}}, commit="add a file")
    tmp_dir.dvc_gen({"dir": {"2": "two"}}, commit="modify a file")

    (tmp_dir / "dir" / "2").unlink()
    dvc.add("dir")
    scm.add("dir.dvc")
    scm.commit("delete a file")

    assert dvc.diff(":/init", ":/directory") == {
        "added": [
            {
                "filename": os.path.join("dir", ""),
                "checksum": "5fb6b29836c388e093ca0715c872fe2a.dir",
            },
            {"filename": os.path.join("dir", "1"), "checksum": digest("1")},
            {"filename": os.path.join("dir", "2"), "checksum": digest("2")},
        ],
        "deleted": [],
        "modified": [],
    }

    assert dvc.diff(":/directory", ":/modify") == {
        "added": [
            {"filename": os.path.join("dir", "3"), "checksum": digest("3")}
        ],
        "deleted": [],
        "modified": [
            {
                "filename": os.path.join("dir", ""),
                "checksum": {
                    "old": "5fb6b29836c388e093ca0715c872fe2a.dir",
                    "new": "9b5faf37366b3370fd98e3e60ca439c1.dir",
                },
            },
            {
                "filename": os.path.join("dir", "2"),
                "checksum": {"old": digest("2"), "new": digest("two")},
            },
        ],
    }

    assert dvc.diff(":/modify", ":/delete") == {
        "added": [],
        "deleted": [
            {"filename": os.path.join("dir", "2"), "checksum": digest("two")}
        ],
        "modified": [
            {
                "filename": os.path.join("dir", ""),
                "checksum": {
                    "old": "9b5faf37366b3370fd98e3e60ca439c1.dir",
                    "new": "83ae82fb367ac9926455870773ff09e6.dir",
                },
            }
        ],
    }


def test_json(tmp_dir, scm, dvc, mocker, capsys):
    assert 0 == main(["diff", "--json"])
    assert not capsys.readouterr().out

    diff = {
        "added": [{"filename": "file", "checksum": digest("text")}],
        "deleted": [],
        "modified": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)
    assert 0 == main(["diff", "--json"])
    assert (
        json.dumps(
            {
                "added": [{"filename": "file", "checksum": digest("text")}],
                "deleted": [],
                "modified": [],
            }
        )
        in capsys.readouterr().out
    )


def test_cli(tmp_dir, scm, dvc, mocker, capsys):
    assert 0 == main(["diff"])
    assert not capsys.readouterr().out

    diff = {
        "added": [{"filename": "file", "checksum": digest("text")}],
        "deleted": [],
        "modified": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)
    assert 0 == main(["diff"])
    assert capsys.readouterr().out == (
        "{green}Added{nc}:\n"
        "    file\n".format(green=colorama.Fore.GREEN, nc=colorama.Fore.RESET)
    )

    diff = {
        "added": [],
        "deleted": [
            {"filename": "bar", "checksum": digest("bar")},
            {"filename": "foo", "checksum": digest("foo")},
        ],
        "modified": [
            {
                "filename": "file",
                "checksum": {"old": digest("first"), "new": digest("second")},
            }
        ],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)
    assert 0 == main(["diff", "--checksums"])
    assert capsys.readouterr().out == (
        "{red}Deleted{nc}:\n"
        "    37b51d19  bar\n"
        "    acbd18db  foo\n"
        "\n"
        "{yellow}Modified{nc}:\n"
        "    8b04d5e3..a9f0e61a  file\n".format(
            red=colorama.Fore.RED,
            yellow=colorama.Fore.YELLOW,
            nc=colorama.Fore.RESET,
        )
    )

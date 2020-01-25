import pytest
import hashlib


def _checksum(text):
    return hashlib.md5(bytes(text, "utf-8")).hexdigest()


def test_no_scm(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "text")

    pytest.skip("TODO: define behavior, should it fail?")


def test_added(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "text")

    result = {
        "file": {
            "old": {},
            "new": {"checksum": _checksum("text"), "size": 4},
            "diff": {"status": "added", "size": 4},
        }
    }

    assert result == dvc.diff()


def test_deleted(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "text", commit="add file")
    (tmp_dir / "file.dvc").unlink()

    result = {
        "file": {
            "old": {"checksum": _checksum("text"), "size": 4},
            "new": {},
            "diff": {"status": "deleted", "size": 4},
        }
    }

    assert result == dvc.diff()


def test_modified(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "first", commit="first version")
    tmp_dir.dvc_gen("file", "second")

    result = {
        "file": {
            "old": {"checksum": _checksum("first"), "size": 6},
            "new": {"checksum": _checksum("second"), "size": 6},
            "diff": {"status": "modified", "size": 0},
        }
    }

    assert result == dvc.diff()


def test_refs(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "first", commit="first version")
    tmp_dir.dvc_gen("file", "second", commit="second version")
    tmp_dir.dvc_gen("file", "third", commit="third version")

    HEAD_2 = _checksum("first")
    HEAD_1 = _checksum("second")
    HEAD = _checksum("third")

    assert dvc.diff("HEAD~1") == {
        "file": {
            "old": {"checksum": HEAD_1, "size": 5},
            "new": {"checksum": HEAD, "size": 5},
            "diff": {"status": "modified", "size": 0},
        }
    }

    assert dvc.diff("HEAD~1", "HEAD~2") == {
        "file": {
            "old": {"checksum": HEAD_2, "size": 5},
            "new": {"checksum": HEAD_1, "size": 5},
            "diff": {"status": "modified", "size": 0},
        }
    }

    pytest.skip('TODO: test dvc.diff("missing")')


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
        "foo": {
            "old": {"checksum": _checksum("foo"), "size": 3},
            "new": {"checksum": _checksum("FOO"), "size": 3},
            "diff": {"status": "modified", "size": 0},
        }
    }

    assert not dvc.diff("HEAD~1", target="missing")


def test_directories(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"dir": {"1": "1", "2": "2"}}, commit="add a directory")
    tmp_dir.dvc_gen({"dir": {"2": "2"}}, commit="delete a file")
    tmp_dir.dvc_gen({"dir": {"2": "two"}}, commit="modify a file")
    tmp_dir.dvc_gen({"dir": {"2": "two", "3": "3"}}, commit="add a file")

    # dvc.diff(":/directory", ":/init")     --> (add: dir/1, dir/2)
    # dvc.diff(":/delete", ":/directory")   --> (deleted: dir/1)
    # dvc.diff(":/modify", ":/directory")   --> (modified: dir/2)
    # dvc.diff(":/modify", ":/delete")      --> (add: dir/3)
    # dvc.diff(":/add a file", ":/modify")
    pytest.skip("TODO: define output structure")


def test_cli(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "text")
    # main(["diff"])
    pytest.skip("TODO: define output structure")


def test_json(tmp_dir, scm, dvc):
    # result = {
    #     "added": {...},
    #     "renamed": {...},
    #     "modified": {...},
    #     "deleted": {...},
    # }

    # main(["diff", "--json"])
    pytest.skip("TODO: define output structure")

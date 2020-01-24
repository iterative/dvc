import pytest


def test_no_scm(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "text")

    pytest.skip("TODO: define behavior, should it fail?")


def test_added(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "text")

    result = {
        "file": {
            "old": {},
            "new": {"checksum": "1cb251ec0d568de6a929b520c4aed8d1", "size": 4},
            "diff": {"status": "added", "size": 4},
        }
    }

    assert result == dvc.diff()


def test_deleted(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "text", commit="add file")
    (tmp_dir / "file.dvc").unlink()

    result = {
        "file": {
            "old": {"checksum": "1cb251ec0d568de6a929b520c4aed8d1", "size": 4},
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
            "old": {"checksum": "8b04d5e3775d298e78455efc5ca404d5", "size": 6},
            "new": {},
            "diff": {"status": "deleted", "size": 6},
        }
    }

    assert result == dvc.diff()


def test_refs(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "first", commit="first version")
    tmp_dir.dvc_gen("file", "second", commit="second version")
    tmp_dir.dvc_gen("file", "third", commit="third version")

    # dvc.diff("HEAD~1") --> (third, second)
    # dvc.diff("HEAD~1", "HEAD~2") --> (second, first)
    # dvc.diff("missing") --> error
    pytest.skip("TODO: define output structure")


def test_target(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    tmp_dir.dvc_gen("bar", "bar")
    scm.add([".gitignore", "foo.dvc", "bar.dvc"])
    scm.commit("lowercase")

    tmp_dir.dvc_gen("foo", "FOO")
    tmp_dir.dvc_gen("bar", "BAR")
    scm.add(["foo.dvc", "bar.dvc"])
    scm.commit("uppercase")

    # dvc.diff("HEAD~1", target="foo")
    # dvc.diff("HEAD~1", target="missing") --> error
    pytest.skip("TODO: define output structure")


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

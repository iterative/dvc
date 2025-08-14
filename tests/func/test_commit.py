import os
import textwrap

import pytest

from dvc.dependency.base import DependencyDoesNotExistError
from dvc.dvcfile import PROJECT_FILE, Lockfile, ProjectFile, SingleStageFile
from dvc.fs import localfs
from dvc.output import OutputDoesNotExistError
from dvc.stage.exceptions import StageCommitError


def test_commit_recursive(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"file": "text1", "subdir": {"file2": "text2"}}})
    stages = dvc.add(localfs.find("dir"), no_commit=True)

    assert len(stages) == 2
    assert dvc.status() != {}

    dvc.commit("dir", recursive=True)
    assert dvc.status() == {}


def test_commit_force(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"file": "text1", "file2": "text2"}})
    (stage,) = dvc.add("dir", no_commit=True)

    assert stage.outs[0].changed_cache()

    tmp_dir.gen("dir/file", "file content modified")

    assert stage.outs[0].changed_cache()

    with pytest.raises(StageCommitError):
        dvc.commit(stage.path)

    assert stage.outs[0].changed_cache()

    dvc.commit(stage.path, force=True)
    assert dvc.status([stage.path]) == {}


def test_commit_preserve_fields(tmp_dir, dvc):
    text = textwrap.dedent(
        """\
        # top comment
        desc: top desc
        outs:
        - path: foo # out comment
          desc: out desc
          type: mytype
          labels:
          - label1
          - label2
          meta:
            key1: value1
            key2: value2
          remote: testremote
          hash: md5
        meta: some metadata
    """
    )
    tmp_dir.gen("foo.dvc", text)
    tmp_dir.dvc_gen("foo", "foo", commit=False)
    dvc.commit("foo")
    assert (tmp_dir / "foo.dvc").read_text() == textwrap.dedent(
        """\
        # top comment
        desc: top desc
        outs:
        - path: foo # out comment
          desc: out desc
          type: mytype
          labels:
          - label1
          - label2
          meta:
            key1: value1
            key2: value2
          remote: testremote
          hash: md5
          md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
        meta: some metadata
    """
    )


def test_commit_with_deps(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    (foo_stage,) = dvc.add("foo", no_commit=True)
    assert foo_stage is not None
    assert len(foo_stage.outs) == 1

    stage = run_copy("foo", "file", no_commit=True, name="copy")
    assert stage is not None
    assert len(stage.outs) == 1

    assert foo_stage.outs[0].changed_cache()
    assert stage.outs[0].changed_cache()

    dvc.commit(stage.path, with_deps=True)
    assert not foo_stage.outs[0].changed_cache()
    assert not stage.outs[0].changed_cache()


def test_commit_changed_md5(tmp_dir, dvc):
    tmp_dir.gen({"file": "file content"})
    (stage,) = dvc.add("file", no_commit=True)

    stage_file_content = (tmp_dir / stage.path).parse()
    stage_file_content["md5"] = "1111111111"
    (tmp_dir / stage.path).dump(stage_file_content)

    with pytest.raises(StageCommitError):
        dvc.commit(stage.path)

    dvc.commit(stage.path, force=True)
    assert "md5" not in (tmp_dir / stage.path).parse()


def test_commit_no_exec(tmp_dir, dvc):
    tmp_dir.gen({"dep": "dep", "out": "out"})
    stage = dvc.run(name="my", cmd="mycmd", deps=["dep"], outs=["out"], no_exec=True)

    assert dvc.status(stage.path)
    dvc.commit(stage.path, force=True)
    assert dvc.status(stage.path) == {}


def test_commit_granular_output(tmp_dir, dvc):
    dvc.run(
        name="mystage",
        cmd=[
            "python -c \"open('foo', 'wb').write(b'foo\\n')\"",
            "python -c \"open('bar', 'wb').write(b'bar\\n')\"",
        ],
        outs=["foo", "bar"],
        no_commit=True,
    )

    cache = tmp_dir / ".dvc" / "cache" / "files" / "md5"
    assert not list(cache.glob("*/*"))

    dvc.commit("foo")
    assert list(cache.glob("*/*")) == [cache / "d3" / "b07384d113edec49eaa6238ad5ff00"]


def test_commit_granular_output_file(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo", no_commit=True)
    dvc.commit("foo")
    assert dvc.status() == {}


def test_commit_granular_output_dir(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "data": {
                "foo": "foo",
                "bar": "bar",
                "subdir": {"subfoo": "subfoo", "subbar": "subbar"},
            }
        }
    )
    dvc.add("data", no_commit=True)
    dvc.commit("data")
    assert dvc.status() == {}


def test_commit_granular_dir(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "data": {
                "foo": "foo",
                "bar": "bar",
                "subdir": {"subfoo": "subfoo", "subbar": "subbar"},
            }
        }
    )
    dvc.add("data", no_commit=True)

    cache = tmp_dir / ".dvc" / "cache" / "files" / "md5"

    assert set(cache.glob("*/*")) == set()

    dvc.commit(os.path.join("data", "foo"))
    assert set(cache.glob("*/*")) == {
        cache / "1a" / "ca2c799df82929bbdd976557975546.dir",
        cache / "ac" / "bd18db4cc2f85cedef654fccc4a4d8",
    }

    dvc.commit(os.path.join("data", "subdir"))
    assert set(cache.glob("*/*")) == {
        cache / "1a" / "ca2c799df82929bbdd976557975546.dir",
        cache / "ac" / "bd18db4cc2f85cedef654fccc4a4d8",
        cache / "4c" / "e8d2a2cf314a52fa7f315ca37ca445",
        cache / "68" / "dde2c3c4e7953c2290f176bbdc9a54",
    }

    dvc.commit(os.path.join("data"))
    assert set(cache.glob("*/*")) == {
        cache / "1a" / "ca2c799df82929bbdd976557975546.dir",
        cache / "ac" / "bd18db4cc2f85cedef654fccc4a4d8",
        cache / "4c" / "e8d2a2cf314a52fa7f315ca37ca445",
        cache / "68" / "dde2c3c4e7953c2290f176bbdc9a54",
        cache / "37" / "b51d194a7513e45b56f6524f2d51f2",
    }


def test_commit_no_exec_missing_dep(tmp_dir, dvc):
    stage = dvc.run(name="my", cmd="mycmd", deps=["dep"], outs=["out"], no_exec=True)
    assert dvc.status(stage.path)

    with pytest.raises(DependencyDoesNotExistError):
        dvc.commit(stage.path, force=True)


def test_commit_no_exec_missing_out(tmp_dir, dvc):
    stage = dvc.run(name="my", cmd="mycmd", outs=["out"], no_exec=True)
    assert dvc.status(stage.path)

    with pytest.raises(OutputDoesNotExistError):
        dvc.commit(stage.path, force=True)


def test_commit_pipeline_stage(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", no_commit=True, name="copy-foo-bar")
    assert dvc.status(stage.addressing)
    assert dvc.commit(stage.addressing, force=True) == [stage]
    assert not dvc.status(stage.addressing)

    # just to confirm different variants work
    assert dvc.commit(f":{stage.addressing}") == [stage]
    assert dvc.commit(f"{PROJECT_FILE}:{stage.addressing}") == [stage]
    assert dvc.commit(PROJECT_FILE) == [stage]


def test_imported_entries_unchanged(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file", "file content", "initial commit")

    stage = dvc.imp(os.fspath(erepo_dir), "file")

    assert stage.changed_entries() == ([], [], None)


def test_commit_updates_to_cloud_versioning_dir(tmp_dir, dvc):
    data_dvc = tmp_dir / "data.dvc"
    data_dvc.dump(
        {
            "outs": [
                {
                    "path": "data",
                    "hash": "md5",
                    "files": [
                        {
                            "size": 3,
                            "version_id": "WYRG4BglP7pD.gEoJP6a4AqOhl.FRA.h",
                            "etag": "acbd18db4cc2f85cedef654fccc4a4d8",
                            "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                            "relpath": "bar",
                        },
                        {
                            "size": 3,
                            "version_id": "0vL53tFVY5vVAoJ4HG2jCS1mEcohDPE0",
                            "etag": "acbd18db4cc2f85cedef654fccc4a4d8",
                            "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                            "relpath": "foo",
                        },
                    ],
                }
            ]
        }
    )

    data = tmp_dir / "data"
    data.mkdir()
    (data / "foo").write_text("foo")
    (data / "bar").write_text("bar2")

    dvc.commit("data", force=True)

    assert (tmp_dir / "data.dvc").parse() == {
        "outs": [
            {
                "path": "data",
                "hash": "md5",
                "files": [
                    {
                        "size": 4,
                        "md5": "224e2539f52203eb33728acd228b4432",
                        "relpath": "bar",
                    },
                    {
                        "size": 3,
                        "version_id": "0vL53tFVY5vVAoJ4HG2jCS1mEcohDPE0",
                        "etag": "acbd18db4cc2f85cedef654fccc4a4d8",
                        "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                        "relpath": "foo",
                    },
                ],
            }
        ]
    }


def test_commit_dos2unix(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    (tmp_dir / "foo.dvc").dump(
        {
            "outs": [
                {"path": "foo", "md5": "acbd18db4cc2f85cedef654fccc4a4d8", "size": 3},
            ]
        }
    )
    legacy_content = (tmp_dir / "foo.dvc").read_text()
    assert "hash: md5" not in legacy_content

    dvc.commit("foo.dvc", force=True)
    assert (tmp_dir / "foo.dvc").read_text() == legacy_content

    tmp_dir.gen("foo", "modified")
    dvc.commit("foo.dvc", force=True)
    content = (tmp_dir / "foo.dvc").read_text()
    assert "hash: md5" in content


def test_commit_multiple_files(tmp_dir, dvc, mocker):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    stages = dvc.add(["foo", "bar"], no_commit=True)
    test1_stage = dvc.stage.add(name="test", cmd="echo test", deps=["foo"])
    test2_stage = dvc.stage.add(name="test2", cmd="echo test2", deps=["foo"])

    subdir = tmp_dir / "subdir"
    subdir.mkdir()
    with subdir.chdir():
        bar_relpath = os.path.relpath(tmp_dir / "bar", subdir)
        test3_stage = dvc.stage.add(name="test3", cmd="echo test3", deps=[bar_relpath])

    pointerfile_spy = mocker.spy(SingleStageFile, "dump_stages")
    projectfile_spy = mocker.spy(ProjectFile, "dump_stages")
    lockfile_spy = mocker.spy(Lockfile, "dump_stages")

    assert set(dvc.commit(force=True)) == {
        *stages,
        test1_stage,
        test2_stage,
        test3_stage,
    }
    pointerfile_spy.assert_has_calls(
        [
            mocker.call(stages[0].dvcfile, [stages[0]], update_pipeline=False),
            mocker.call(stages[1].dvcfile, [stages[1]], update_pipeline=False),
        ],
        any_order=True,
    )
    projectfile_spy.assert_has_calls(
        [
            mocker.call(
                test1_stage.dvcfile, [test1_stage, test2_stage], update_pipeline=False
            ),
            mocker.call(test3_stage.dvcfile, [test3_stage], update_pipeline=False),
        ],
        any_order=True,
    )
    lockfile_spy.assert_has_calls(
        [
            mocker.call(test1_stage.dvcfile._lockfile, [test1_stage, test2_stage]),
            mocker.call(test3_stage.dvcfile._lockfile, [test3_stage]),
        ],
        any_order=True,
    )
    assert dvc.status() == {}

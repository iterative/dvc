import logging
import os
import shutil
from os.path import join

import pytest

import dvc_data
from dvc.cli import main
from dvc.dvcfile import FileMixin, SingleStageFile
from dvc.exceptions import CheckoutError
from dvc.repo.open_repo import clean_repos
from dvc.scm import CloneError
from dvc.stage.exceptions import StageNotFound
from dvc.testing.remote_tests import TestRemote  # noqa: F401
from dvc.utils.fs import remove
from dvc_data.hashfile.db import HashFileDB
from dvc_data.hashfile.db.local import LocalHashFileDB
from dvc_data.hashfile.hash_info import HashInfo
from tests.func.test_checkout import empty_checkout, empty_stats

empty_stats = empty_stats | {"fetched": 0}
empty_pull = empty_checkout | {"stats": empty_stats}


def test_cloud_cli(tmp_dir, dvc, capsys, remote, mocker):  # noqa: PLR0915
    jobs = 2
    args = ["-v", "-j", str(jobs)]

    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    cache = stage.outs[0].cache_path

    (stage_dir,) = tmp_dir.dvc_gen(
        {
            "data_dir": {
                "data_sub_dir": {"data_sub": "data_sub"},
                "data": "data",
                "empty": "",
            }
        }
    )
    assert stage_dir is not None
    cache_dir = stage_dir.outs[0].cache_path

    # FIXME check status output
    oids_exist = mocker.spy(LocalHashFileDB, "oids_exist")

    assert main(["push", *args]) == 0
    assert capsys.readouterr().out == "5 files pushed\n"
    assert os.path.exists(cache)
    assert os.path.isfile(cache)
    assert os.path.isfile(cache_dir)
    assert oids_exist.called
    assert all(
        _kwargs["jobs"] == jobs for (_args, _kwargs) in oids_exist.call_args_list
    )

    dvc.cache.local.clear()
    oids_exist.reset_mock()

    assert main(["fetch", *args]) == 0
    assert capsys.readouterr().out == "5 files fetched\n"
    assert os.path.exists(cache)
    assert os.path.isfile(cache)
    assert os.path.isfile(cache_dir)
    assert oids_exist.called
    assert all(
        _kwargs["jobs"] == jobs for (_args, _kwargs) in oids_exist.call_args_list
    )

    oids_exist.reset_mock()

    assert main(["pull", *args]) == 0
    assert capsys.readouterr().out == "Everything is up to date.\n"
    assert os.path.exists(cache)
    assert os.path.isfile(cache)
    assert os.path.isfile(cache_dir)
    assert os.path.isfile("foo")
    assert os.path.isdir("data_dir")
    assert oids_exist.called
    assert all(
        _kwargs["jobs"] == jobs for (_args, _kwargs) in oids_exist.call_args_list
    )

    with open(cache, encoding="utf-8") as fd:
        assert fd.read() == "foo"
    assert os.path.isfile(cache_dir)

    # NOTE: http doesn't support gc yet
    if remote.url.startswith("http"):
        return

    oids_exist.reset_mock()

    _list_oids_traverse = mocker.spy(HashFileDB, "_list_oids_traverse")
    # NOTE: check if remote gc works correctly on directories
    assert main(["gc", "-cw", "-f", *args]) == 0
    assert _list_oids_traverse.called
    assert all(_kwargs["jobs"] == 2 for (_args, _kwargs) in oids_exist.call_args_list)
    shutil.move(dvc.cache.local.path, dvc.cache.local.path + ".back")

    assert main(["fetch", *args]) == 0
    assert capsys.readouterr().out == "5 files fetched\n"

    assert oids_exist.called
    assert all(
        _kwargs["jobs"] == jobs for (_args, _kwargs) in oids_exist.call_args_list
    )

    oids_exist.reset_mock()
    assert main(["pull", "-f", *args]) == 0
    assert capsys.readouterr().out == "Everything is up to date.\n"
    assert os.path.exists(cache)
    assert os.path.isfile(cache)
    assert os.path.isfile(cache_dir)
    assert os.path.isfile("foo")
    assert os.path.isdir("data_dir")
    assert oids_exist.called
    assert all(
        _kwargs["jobs"] == jobs for (_args, _kwargs) in oids_exist.call_args_list
    )


def test_data_cloud_error_cli(dvc):
    f = "non-existing-file"
    assert main(["status", "-c", f])
    assert main(["push", f])
    assert main(["pull", f])
    assert main(["fetch", f])


def test_warn_on_outdated_stage(tmp_dir, dvc, local_remote, caplog):
    stage = dvc.run(outs=["bar"], cmd="echo bar > bar", name="gen-bar")
    dvc.push()

    stage.outs[0].hash_info = HashInfo()
    stage.dump()

    with caplog.at_level(logging.WARNING, logger="dvc"):
        caplog.clear()
        assert main(["status", "-c"]) == 0
        expected_warning = (
            "Output 'bar'(stage: 'gen-bar') is missing version info. "
            "Cache for it will not be collected. "
            "Use `dvc repro` to get your pipeline up to date."
        )

        assert expected_warning in caplog.text


def test_hash_recalculation(mocker, dvc, tmp_dir, local_remote):
    tmp_dir.gen({"foo": "foo"})
    test_file_md5 = mocker.spy(dvc_data.hashfile.hash, "file_md5")
    ret = main(["config", "cache.type", "hardlink"])
    assert ret == 0
    ret = main(["add", "foo"])
    assert ret == 0
    ret = main(["push"])
    assert ret == 0
    assert test_file_md5.mock.call_count == 3


def test_missing_cache(tmp_dir, dvc, local_remote, caplog):
    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})

    # purge cache
    dvc.cache.local.clear()

    header = (
        "Some of the cache files do not exist "
        "neither locally nor on remote. Missing cache files:\n"
    )
    foo = "md5: 37b51d194a7513e45b56f6524f2d51f2\n"
    bar = "md5: acbd18db4cc2f85cedef654fccc4a4d8\n"

    caplog.clear()
    dvc.push()
    assert header in caplog.text
    assert foo in caplog.text
    assert bar in caplog.text

    caplog.clear()
    dvc.fetch()
    assert header in caplog.text
    assert foo in caplog.text
    assert bar in caplog.text

    caplog.clear()
    assert dvc.status(cloud=True) == {"bar": "missing", "foo": "missing"}
    assert header not in caplog.text
    assert foo not in caplog.text
    assert bar not in caplog.text


def test_verify_hashes(tmp_dir, scm, dvc, mocker, tmp_path_factory, local_remote):
    tmp_dir.dvc_gen({"file": "file1 content"}, commit="add file")
    tmp_dir.dvc_gen({"dir": {"subfile": "file2 content"}}, commit="add dir")
    dvc.push()

    # remove artifacts and cache to trigger fetching
    remove("file")
    remove("dir")
    dvc.cache.local.clear()

    hash_spy = mocker.spy(dvc_data.hashfile.hash, "file_md5")

    assert dvc.pull() == empty_pull | {
        "added": ["dir" + os.sep, "file"],
        "stats": empty_stats | {"fetched": 3, "added": 2},
    }

    # NOTE: 2 are for index.data_tree building
    assert hash_spy.call_count == 3

    # Removing cache will invalidate existing state entries
    dvc.cache.local.clear()

    with dvc.config.edit() as conf:
        conf["remote"]["upstream"]["verify"] = True

    assert dvc.pull() == empty_pull | {"stats": empty_stats | {"fetched": 3}}
    assert hash_spy.call_count == 10


# @pytest.mark.flaky(reruns=3)
@pytest.mark.parametrize("erepo_type", ["git_dir", "erepo_dir"])
def test_pull_git_imports(request, tmp_dir, dvc, scm, erepo_type):
    erepo = request.getfixturevalue(erepo_type)
    with erepo.chdir():
        erepo.scm_gen({"dir": {"bar": "bar"}}, commit="second")
        erepo.scm_gen("foo", "foo", commit="first")

    dvc.imp(os.fspath(erepo), "foo")
    dvc.imp(os.fspath(erepo), "dir", out="new_dir", rev="HEAD~")

    assert dvc.pull() == empty_pull

    for item in ["foo", "new_dir"]:
        remove(item)
    dvc.cache.local.clear()
    os.makedirs(dvc.cache.local.path, exist_ok=True)
    clean_repos()

    assert dvc.pull(force=True) == empty_pull | {
        "added": ["new_dir" + os.sep, "foo"],
        "stats": empty_stats | {"fetched": 2, "added": 2},
    }

    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / "foo").read_text() == "foo"

    assert (tmp_dir / "new_dir").exists()
    assert (tmp_dir / "new_dir" / "bar").read_text() == "bar"


def test_pull_external_dvc_imports(tmp_dir, dvc, scm, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen({"dir": {"bar": "bar"}}, commit="second")
        erepo_dir.dvc_gen("foo", "foo", commit="first")

        os.remove("foo")
        shutil.rmtree("dir")

    dvc.imp(os.fspath(erepo_dir), "foo")
    dvc.imp(os.fspath(erepo_dir), "dir", out="new_dir", rev="HEAD~")

    assert dvc.pull() == empty_pull

    clean(["foo", "new_dir"], dvc)

    assert dvc.pull(force=True) == empty_pull | {
        "added": ["new_dir" + os.sep, "foo"],
        "stats": empty_stats | {"fetched": 2, "added": 2},
    }

    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / "foo").read_text() == "foo"

    assert (tmp_dir / "new_dir").exists()
    assert (tmp_dir / "new_dir" / "bar").read_text() == "bar"


def test_pull_partial_import(tmp_dir, dvc, local_workspace):
    local_workspace.gen("file", "file content")
    dst = tmp_dir / "file"
    stage = dvc.imp_url("remote://workspace/file", os.fspath(dst), no_download=True)

    result = dvc.pull("file")
    assert result == empty_checkout | {
        "added": ["file"],
        "stats": empty_stats | {"fetched": 1, "added": 1},
    }
    assert dst.exists()

    assert stage.outs[0].get_hash().value == "d10b4c3ff123b26dc068d43a8bef2d23"


def test_pull_partial_import_missing(tmp_dir, dvc, local_workspace):
    local_workspace.gen("file", "file content")
    dst = tmp_dir / "file"
    dvc.imp_url("remote://workspace/file", os.fspath(dst), no_download=True)

    (local_workspace / "file").unlink()
    with pytest.raises(CheckoutError) as exc:
        dvc.pull("file")
    assert exc.value.result == empty_pull | {"failed": ["file"]}
    assert not dst.exists()


def test_pull_partial_import_modified(tmp_dir, dvc, local_workspace):
    local_workspace.gen("file", "file content")
    dst = tmp_dir / "file"
    dvc.imp_url("remote://workspace/file", os.fspath(dst), no_download=True)

    local_workspace.gen("file", "updated file content")
    with pytest.raises(CheckoutError) as exc:
        dvc.pull("file")
    assert exc.value.result == empty_pull | {"failed": ["file"]}
    assert not dst.exists()


def test_pull_external_dvc_imports_mixed(tmp_dir, dvc, scm, erepo_dir, local_remote):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo", commit="first")
        os.remove("foo")

    # imported: foo
    dvc.imp(os.fspath(erepo_dir), "foo")

    # local-object: bar
    tmp_dir.dvc_gen("bar", "bar")
    dvc.push("bar")

    clean(["foo", "bar"], dvc)

    assert dvc.pull() == empty_pull | {
        "added": ["bar", "foo"],
        "stats": empty_stats | {"fetched": 2, "added": 2},
    }
    assert (tmp_dir / "foo").read_text() == "foo"
    assert (tmp_dir / "bar").read_text() == "bar"


def clean(outs, dvc=None):
    if dvc:
        dvc.cache.local.clear()
    for path in outs:
        remove(path)
    if dvc:
        clean_repos()


def recurse_list_dir(d):
    return [
        os.path.join(root, f) for root, _, filenames in os.walk(d) for f in filenames
    ]


def test_dvc_pull_pipeline_stages(tmp_dir, dvc, run_copy, local_remote):
    (stage0,) = tmp_dir.dvc_gen("bar", "bar")
    stage1 = run_copy("bar", "foo", name="copy-bar-foo")
    stage2 = run_copy("foo", "foobar", name="copy-foo-foobar")
    dvc.push()

    outs = ["bar", "foo", "foobar"]

    clean(outs, dvc)
    assert dvc.pull() == empty_pull | {
        "added": outs,
        "stats": empty_stats | {"fetched": 1, "added": 3},
    }
    assert all((tmp_dir / file).exists() for file in outs)

    for out, stage in zip(outs, [stage0, stage1, stage2]):
        for target in [stage.addressing, out]:
            clean(outs, dvc)
            stats = dvc.pull([target])
            assert stats == empty_pull | {
                "added": [out],
                "stats": empty_stats | {"fetched": 1, "added": 1},
            }
            assert os.path.exists(out)
            assert not any(os.path.exists(out) for out in set(outs) - {out})

    clean(outs, dvc)
    stats = dvc.pull([stage2.addressing], with_deps=True)
    assert stats == empty_pull | {
        "added": outs,
        "stats": empty_stats | {"fetched": 1, "added": 3},
    }

    clean(outs, dvc)
    stats = dvc.pull([os.curdir], recursive=True)
    assert stats == empty_pull | {
        "added": outs,
        "stats": empty_stats | {"fetched": 1, "added": 3},
    }


def test_pipeline_file_target_ops(tmp_dir, dvc, run_copy, local_remote):
    path = local_remote.url
    tmp_dir.dvc_gen("foo", "foo")

    tmp_dir.dvc_gen("lorem", "lorem")
    run_copy("lorem", "lorem2", name="copy-lorem-lorem2")

    tmp_dir.dvc_gen("ipsum", "ipsum")
    run_copy("ipsum", "baz", name="copy-ipsum-baz")

    outs = ["foo", "lorem", "ipsum", "baz", "lorem2"]

    remove(dvc.stage_cache.cache_dir)

    assert dvc.push() == 3

    outs = ["foo", "lorem", "ipsum", "baz", "lorem2"]

    # each one's a copy of other, hence 3
    assert len(recurse_list_dir(path)) == 3

    clean(outs, dvc)
    assert dvc.pull(["dvc.yaml"]) == empty_pull | {
        "added": ["baz", "lorem2"],
        "stats": empty_stats | {"fetched": 2, "added": 2},
    }

    clean(outs, dvc)
    assert dvc.pull() == empty_pull | {
        "added": ["baz", "foo", "ipsum", "lorem", "lorem2"],
        "stats": empty_stats | {"fetched": 3, "added": 5},
    }

    # clean everything in remote and push
    from dvc.testing.tmp_dir import TmpDir

    clean(TmpDir(path).iterdir())
    assert dvc.push(["dvc.yaml:copy-ipsum-baz"]) == 1
    assert len(recurse_list_dir(path)) == 1

    clean(TmpDir(path).iterdir())
    assert dvc.push(["dvc.yaml"]) == 2
    assert len(recurse_list_dir(path)) == 2

    with pytest.raises(StageNotFound):
        dvc.push(["dvc.yaml:StageThatDoesNotExist"])

    with pytest.raises(StageNotFound):
        dvc.pull(["dvc.yaml:StageThatDoesNotExist"])


@pytest.mark.parametrize(
    "fs, msg",
    [
        ({"foo": "foo", "bar": "bar"}, "2 files pushed"),
        ({"foo": "foo"}, "1 file pushed"),
        ({}, "Everything is up to date"),
    ],
)
def test_push_stats(tmp_dir, dvc, fs, msg, capsys, local_remote):
    tmp_dir.dvc_gen(fs)

    main(["push"])
    out, _ = capsys.readouterr()
    assert msg in out


@pytest.mark.parametrize(
    "fs, msg",
    [
        ({"foo": "foo", "bar": "bar"}, "2 files fetched"),
        ({"foo": "foo"}, "1 file fetched"),
        ({}, "Everything is up to date."),
    ],
)
def test_fetch_stats(tmp_dir, dvc, fs, msg, capsys, local_remote):
    tmp_dir.dvc_gen(fs)
    dvc.push()
    clean(list(fs.keys()), dvc)

    main(["fetch"])
    out, _ = capsys.readouterr()
    assert msg in out


def test_pull_stats(tmp_dir, dvc, capsys, local_remote):
    tmp_dir.dvc_gen(
        {
            "foo": "foo",
            "bar": "bar",
            "lorem": "lorem",
            "dir": {"file": "file"},
            "ipsum": "ipsum",
            "dolor": "dolor",
        }
    )
    dvc.push()
    clean(["foo", "bar", "dir", "lorem"], dvc)

    (tmp_dir / "ipsum.dvc").unlink()
    (tmp_dir / "bar").write_text("foobar")

    assert main(["pull", "--force"]) == 0
    out, _ = capsys.readouterr()
    assert out.splitlines() == [
        "M\tbar".expandtabs(),
        "A\tdir".expandtabs() + os.sep,
        "A\tfoo".expandtabs(),
        "A\tlorem".expandtabs(),
        "D\tipsum".expandtabs(),
        "6 files fetched, 1 file modified, 3 files added and 1 file deleted",
    ]

    main(["pull"])
    out, _ = capsys.readouterr()
    assert out == "Everything is up to date.\n"


@pytest.mark.parametrize(
    "key,expected", [("all_tags", 2), ("all_branches", 3), ("all_commits", 3)]
)
def test_push_pull_all(tmp_dir, scm, dvc, local_remote, key, expected):
    tmp_dir.dvc_gen({"foo": "foo"}, commit="first")
    scm.tag("v1")
    dvc.remove("foo.dvc")
    tmp_dir.dvc_gen({"bar": "bar"}, commit="second")
    scm.tag("v2")
    with tmp_dir.branch("branch", new=True):
        dvc.remove("bar.dvc")
        tmp_dir.dvc_gen({"baz": "baz"}, commit="branch")

    assert dvc.push(**{key: True}) == expected

    clean(["foo", "bar", "baz"], dvc)
    assert dvc.pull(**{key: True}) == empty_pull | {
        "added": ["bar", "foo"],
        "stats": empty_stats | {"fetched": expected, "added": 2},
    }


def test_push_pull_fetch_pipeline_stages(tmp_dir, dvc, run_copy, local_remote):
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")

    assert dvc.push("copy-foo-bar") == 1
    assert len(recurse_list_dir(local_remote.url)) == 1
    # pushing everything so as we can check pull/fetch only downloads
    # from specified targets
    assert dvc.push() == 0
    clean(["foo", "bar"], dvc)

    assert dvc.pull("copy-foo-bar") == empty_pull | {
        "added": ["bar"],
        "stats": empty_stats | {"fetched": 1, "added": 1},
    }
    assert (tmp_dir / "bar").exists()
    assert len(recurse_list_dir(dvc.cache.local.path)) == 1
    clean(["bar"], dvc)

    assert dvc.fetch("copy-foo-bar") == 1
    assert len(recurse_list_dir(dvc.cache.local.path)) == 1


def test_pull_partial(tmp_dir, dvc, local_remote):
    other_files = {f"spam{i}": f"spam{i}" for i in range(10)}
    tmp_dir.dvc_gen({"foo": {"bar": {"baz": "baz"}, **other_files}})
    dvc.push()
    clean(["foo"], dvc)

    stats = dvc.pull(os.path.join("foo", "bar"))
    assert stats == empty_pull | {
        "added": [os.path.join("foo", "")],
        "stats": empty_stats | {"fetched": 2, "added": 1},
    }
    assert (tmp_dir / "foo").read_text() == {"bar": {"baz": "baz"}}


def test_output_remote(tmp_dir, dvc, make_remote):
    make_remote("default", default=True)
    make_remote("for_foo", default=False)
    make_remote("for_data", default=False)

    tmp_dir.dvc_gen("foo", "foo")
    tmp_dir.dvc_gen("bar", "bar")
    tmp_dir.dvc_gen("data", {"one": "one", "two": "two"})

    with (tmp_dir / "foo.dvc").modify() as d:
        d["outs"][0]["remote"] = "for_foo"

    with (tmp_dir / "data.dvc").modify() as d:
        d["outs"][0]["remote"] = "for_data"

    dvc.push()

    default = dvc.cloud.get_remote_odb("default")
    for_foo = dvc.cloud.get_remote_odb("for_foo")
    for_data = dvc.cloud.get_remote_odb("for_data")

    assert set(default.all()) == {"37b51d194a7513e45b56f6524f2d51f2"}
    assert set(for_foo.all()) == {"acbd18db4cc2f85cedef654fccc4a4d8"}
    assert set(for_data.all()) == {
        "f97c5d29941bfb1b2fdab0874906ab82",
        "6b18131dc289fd37006705affe961ef8.dir",
        "b8a9f715dbb64fd5c56e7783c6820a61",
    }

    clean(["foo", "bar", "data"], dvc)

    assert dvc.pull() == empty_pull | {
        "added": ["data" + os.sep, "bar", "foo"],
        "stats": empty_stats | {"fetched": 5, "added": 4},
    }

    assert set(dvc.cache.local.all()) == {
        "37b51d194a7513e45b56f6524f2d51f2",
        "acbd18db4cc2f85cedef654fccc4a4d8",
        "f97c5d29941bfb1b2fdab0874906ab82",
        "6b18131dc289fd37006705affe961ef8.dir",
        "b8a9f715dbb64fd5c56e7783c6820a61",
    }


def test_target_remote(tmp_dir, dvc, make_remote):
    make_remote("default", default=True)
    make_remote("myremote", default=False)

    tmp_dir.dvc_gen("foo", "foo")
    tmp_dir.dvc_gen("data", {"one": "one", "two": "two"})

    dvc.push(remote="myremote")

    default = dvc.cloud.get_remote_odb("default")
    myremote = dvc.cloud.get_remote_odb("myremote")

    assert set(default.all()) == set()
    assert set(myremote.all()) == {
        "acbd18db4cc2f85cedef654fccc4a4d8",
        "f97c5d29941bfb1b2fdab0874906ab82",
        "6b18131dc289fd37006705affe961ef8.dir",
        "b8a9f715dbb64fd5c56e7783c6820a61",
    }

    clean(["foo", "data"], dvc)

    assert dvc.pull(remote="myremote") == empty_pull | {
        "added": ["data" + os.sep, "foo"],
        "stats": empty_stats | {"fetched": 4, "added": 3},
    }

    assert set(dvc.cache.local.all()) == {
        "acbd18db4cc2f85cedef654fccc4a4d8",
        "f97c5d29941bfb1b2fdab0874906ab82",
        "6b18131dc289fd37006705affe961ef8.dir",
        "b8a9f715dbb64fd5c56e7783c6820a61",
    }


def test_output_target_remote(tmp_dir, dvc, make_remote):
    make_remote("default", default=True)
    make_remote("for_foo", default=False)
    make_remote("for_bar", default=False)

    tmp_dir.dvc_gen("foo", "foo")
    tmp_dir.dvc_gen("bar", "bar")
    tmp_dir.dvc_gen("data", {"one": "one", "two": "two"})

    with (tmp_dir / "foo.dvc").modify() as d:
        d["outs"][0]["remote"] = "for_foo"

    with (tmp_dir / "bar.dvc").modify() as d:
        d["outs"][0]["remote"] = "for_bar"

    # push foo and data to for_foo remote
    dvc.push(remote="for_foo")

    default = dvc.cloud.get_remote_odb("default")
    for_foo = dvc.cloud.get_remote_odb("for_foo")
    for_bar = dvc.cloud.get_remote_odb("for_bar")

    # hashes for foo and data, but not bar
    expected = {
        "acbd18db4cc2f85cedef654fccc4a4d8",
        "f97c5d29941bfb1b2fdab0874906ab82",
        "6b18131dc289fd37006705affe961ef8.dir",
        "b8a9f715dbb64fd5c56e7783c6820a61",
    }

    assert set(default.all()) == set()
    assert set(for_foo.all()) == expected
    assert set(for_bar.all()) == set()

    # push everything without specifying remote
    dvc.push()
    assert set(default.all()) == {
        "f97c5d29941bfb1b2fdab0874906ab82",
        "6b18131dc289fd37006705affe961ef8.dir",
        "b8a9f715dbb64fd5c56e7783c6820a61",
    }
    assert set(for_foo.all()) == expected
    assert set(for_bar.all()) == {"37b51d194a7513e45b56f6524f2d51f2"}

    clean(["foo", "bar", "data"], dvc)

    # pull foo and data from for_foo remote
    assert dvc.pull(remote="for_foo", allow_missing=True) == empty_pull | {
        "added": ["data" + os.sep, "foo"],
        "stats": empty_stats | {"fetched": 4, "added": 3},
    }

    assert set(dvc.cache.local.all()) == expected


def test_pull_allow_missing(tmp_dir, dvc, local_remote):
    dvc.stage.add(name="bar", outs=["bar"], cmd="echo bar > bar")

    with pytest.raises(CheckoutError) as exc:
        dvc.pull()
    assert exc.value.result == empty_pull | {"failed": ["bar"]}

    tmp_dir.dvc_gen("foo", "foo")
    dvc.push()
    clean(["foo"], dvc)

    stats = dvc.pull(allow_missing=True)
    assert stats == empty_pull | {
        "added": ["foo"],
        "stats": empty_stats | {"fetched": 1, "added": 1},
    }


def test_pull_granular_excluding_import_that_cannot_be_pulled(
    tmp_dir, dvc, local_remote, mocker
):
    """Regression test for https://github.com/iterative/dvc/issues/10309."""

    mocker.patch("dvc.fs.dvc._DVCFileSystem", side_effect=CloneError("SCM error"))
    (stage,) = tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    imp_stage = dvc.imp(
        "https://user:token@github.com/iterative/dvc.git",
        "dir",
        out="new_dir",
        rev="HEAD",
        no_exec=True,
    )
    dvc.push()

    shutil.rmtree("dir")
    dvc.cache.local.clear()

    assert dvc.pull(stage.addressing) == empty_pull | {
        "added": [join("dir", "")],
        "stats": empty_stats | {"added": 2, "fetched": 3},
    }

    with pytest.raises(CloneError, match="SCM error"):
        dvc.pull()
    with pytest.raises(CloneError, match="SCM error"):
        dvc.pull(imp_stage.addressing)


def test_loads_single_file(tmp_dir, dvc, local_remote, mocker):
    tmp_dir.dvc_gen("foo", "foo")
    tmp_dir.dvc_gen("bar", "bar")

    foo_dvcfile = SingleStageFile(dvc, "foo.dvc")
    bar_dvcfile = SingleStageFile(dvc, "bar.dvc")

    spy = mocker.spy(FileMixin, "_load")
    assert dvc.push("foo.dvc") == 1
    spy.assert_called_with(foo_dvcfile)
    spy.reset_mock()

    assert dvc.push("bar.dvc") == 1
    spy.assert_called_with(bar_dvcfile)
    spy.reset_mock()

    dvc.cache.local.clear()
    (tmp_dir / "bar").unlink()
    (tmp_dir / "foo").unlink()

    assert dvc.pull("foo.dvc") == {
        "added": ["foo"],
        "deleted": [],
        "modified": [],
        "stats": {"added": 1, "deleted": 0, "modified": 0, "fetched": 1},
    }
    spy.assert_called_with(foo_dvcfile)
    assert (tmp_dir / "foo").exists()
    assert not (tmp_dir / "bar").exists()
    spy.reset_mock()

    assert dvc.fetch("bar.dvc") == 1
    spy.assert_called_with(bar_dvcfile)

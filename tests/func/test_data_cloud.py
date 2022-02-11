import logging
import os
import shutil

import pytest
from flaky.flaky_decorator import flaky

import dvc as dvc_module
from dvc.cli import main
from dvc.data.db.local import LocalObjectDB
from dvc.external_repo import clean_repos
from dvc.objects.db import ObjectDB
from dvc.stage.exceptions import StageNotFound
from dvc.testing.test_remote import (  # noqa, pylint: disable=unused-import
    TestRemote,
)
from dvc.utils.fs import remove


def test_cloud_cli(tmp_dir, dvc, remote, mocker):
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
    hashes_exist = mocker.spy(LocalObjectDB, "hashes_exist")

    assert main(["push"] + args) == 0
    assert os.path.exists(cache)
    assert os.path.isfile(cache)
    assert os.path.isfile(cache_dir)
    assert hashes_exist.called
    assert all(
        _kwargs["jobs"] == jobs
        for (_args, _kwargs) in hashes_exist.call_args_list
    )

    remove(dvc.odb.local.cache_dir)
    hashes_exist.reset_mock()

    assert main(["fetch"] + args) == 0
    assert os.path.exists(cache)
    assert os.path.isfile(cache)
    assert os.path.isfile(cache_dir)
    assert hashes_exist.called
    assert all(
        _kwargs["jobs"] == jobs
        for (_args, _kwargs) in hashes_exist.call_args_list
    )

    hashes_exist.reset_mock()

    assert main(["pull"] + args) == 0
    assert os.path.exists(cache)
    assert os.path.isfile(cache)
    assert os.path.isfile(cache_dir)
    assert os.path.isfile("foo")
    assert os.path.isdir("data_dir")
    assert hashes_exist.called
    assert all(
        _kwargs["jobs"] == jobs
        for (_args, _kwargs) in hashes_exist.call_args_list
    )

    with open(cache, encoding="utf-8") as fd:
        assert fd.read() == "foo"
    assert os.path.isfile(cache_dir)

    # NOTE: http doesn't support gc yet
    if remote.url.startswith("http"):
        return

    hashes_exist.reset_mock()

    _list_hashes_traverse = mocker.spy(ObjectDB, "_list_hashes_traverse")
    # NOTE: check if remote gc works correctly on directories
    assert main(["gc", "-cw", "-f"] + args) == 0
    assert _list_hashes_traverse.called
    assert all(
        _kwargs["jobs"] == 2
        for (_args, _kwargs) in hashes_exist.call_args_list
    )
    shutil.move(dvc.odb.local.cache_dir, dvc.odb.local.cache_dir + ".back")

    assert main(["fetch"] + args) == 0

    assert hashes_exist.called
    assert all(
        _kwargs["jobs"] == jobs
        for (_args, _kwargs) in hashes_exist.call_args_list
    )

    hashes_exist.reset_mock()
    assert main(["pull", "-f"] + args) == 0
    assert os.path.exists(cache)
    assert os.path.isfile(cache)
    assert os.path.isfile(cache_dir)
    assert os.path.isfile("foo")
    assert os.path.isdir("data_dir")
    assert hashes_exist.called
    assert all(
        _kwargs["jobs"] == jobs
        for (_args, _kwargs) in hashes_exist.call_args_list
    )


def test_data_cloud_error_cli(dvc):
    f = "non-existing-file"
    assert main(["status", "-c", f])
    assert main(["push", f])
    assert main(["pull", f])
    assert main(["fetch", f])


def test_warn_on_outdated_stage(tmp_dir, dvc, local_remote, caplog):
    stage = dvc.run(outs=["bar"], cmd="echo bar > bar", single_stage=True)
    assert main(["push"]) == 0

    stage_file_path = stage.relpath
    content = (tmp_dir / stage_file_path).parse()
    del content["outs"][0]["md5"]
    (tmp_dir / stage_file_path).dump(content)

    with caplog.at_level(logging.WARNING, logger="dvc"):
        caplog.clear()
        assert main(["status", "-c"]) == 0
        expected_warning = (
            "Output 'bar'(stage: 'bar.dvc') is missing version info. "
            "Cache for it will not be collected. "
            "Use `dvc repro` to get your pipeline up to date."
        )

        assert expected_warning in caplog.text


def test_hash_recalculation(mocker, dvc, tmp_dir, local_remote):
    tmp_dir.gen({"foo": "foo"})
    test_file_md5 = mocker.spy(dvc_module.data.stage, "file_md5")
    ret = main(["config", "cache.type", "hardlink"])
    assert ret == 0
    ret = main(["add", "foo"])
    assert ret == 0
    ret = main(["push"])
    assert ret == 0
    ret = main(["run", "--single-stage", "-d", "foo", "echo foo"])
    assert ret == 0
    assert test_file_md5.mock.call_count == 1


def test_missing_cache(tmp_dir, dvc, local_remote, caplog):
    from tests.utils import clean_staging

    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})

    # purge cache
    remove(dvc.odb.local.cache_dir)
    clean_staging()

    header = (
        "Some of the cache files do not exist "
        "neither locally nor on remote. Missing cache files:\n"
    )
    foo = "name: bar, md5: 37b51d194a7513e45b56f6524f2d51f2\n"
    bar = "name: foo, md5: acbd18db4cc2f85cedef654fccc4a4d8\n"

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
    assert dvc.status(cloud=True) == {
        "bar": "missing",
        "foo": "missing",
    }
    assert header not in caplog.text
    assert foo not in caplog.text
    assert bar not in caplog.text


def test_verify_hashes(
    tmp_dir, scm, dvc, mocker, tmp_path_factory, local_remote
):
    tmp_dir.dvc_gen({"file": "file1 content"}, commit="add file")
    tmp_dir.dvc_gen({"dir": {"subfile": "file2 content"}}, commit="add dir")
    dvc.push()

    # remove artifacts and cache to trigger fetching
    remove("file")
    remove("dir")
    remove(dvc.odb.local.cache_dir)

    hash_spy = mocker.spy(dvc_module.data.stage, "file_md5")

    dvc.pull()
    assert hash_spy.call_count == 0

    # Removing cache will invalidate existing state entries
    remove(dvc.odb.local.cache_dir)

    dvc.config["remote"]["upstream"]["verify"] = True

    dvc.pull()
    assert hash_spy.call_count == 3


@flaky(max_runs=3, min_passes=1)
@pytest.mark.parametrize(
    "erepo", [pytest.lazy_fixture("git_dir"), pytest.lazy_fixture("erepo_dir")]
)
def test_pull_git_imports(tmp_dir, dvc, scm, erepo):
    with erepo.chdir():
        erepo.scm_gen({"dir": {"bar": "bar"}}, commit="second")
        erepo.scm_gen("foo", "foo", commit="first")

    dvc.imp(os.fspath(erepo), "foo")
    dvc.imp(os.fspath(erepo), "dir", out="new_dir", rev="HEAD~")

    assert dvc.pull()["fetched"] == 0

    for item in ["foo", "new_dir", dvc.odb.local.cache_dir]:
        remove(item)
    os.makedirs(dvc.odb.local.cache_dir, exist_ok=True)
    clean_repos()

    assert dvc.pull(force=True)["fetched"] == 3

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

    assert dvc.pull()["fetched"] == 0

    clean(["foo", "new_dir"], dvc)

    assert dvc.pull(force=True)["fetched"] == 2

    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / "foo").read_text() == "foo"

    assert (tmp_dir / "new_dir").exists()
    assert (tmp_dir / "new_dir" / "bar").read_text() == "bar"


def test_pull_external_dvc_imports_mixed(
    tmp_dir, dvc, scm, erepo_dir, local_remote
):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo", commit="first")
        os.remove("foo")

    # imported: foo
    dvc.imp(os.fspath(erepo_dir), "foo")

    # local-object: bar
    tmp_dir.dvc_gen("bar", "bar")
    dvc.push("bar")

    clean(["foo", "bar"], dvc)

    assert dvc.pull()["fetched"] == 2
    assert (tmp_dir / "foo").read_text() == "foo"
    assert (tmp_dir / "bar").read_text() == "bar"


def clean(outs, dvc=None):
    from tests.utils import clean_staging

    if dvc:
        outs = outs + [dvc.odb.local.cache_dir]
    for path in outs:
        print(path)
        remove(path)
    if dvc:
        os.makedirs(dvc.odb.local.cache_dir, exist_ok=True)
        clean_repos()
        clean_staging()


def recurse_list_dir(d):
    return [
        os.path.join(d, f) for _, _, filenames in os.walk(d) for f in filenames
    ]


def test_dvc_pull_pipeline_stages(tmp_dir, dvc, run_copy, local_remote):
    (stage0,) = tmp_dir.dvc_gen("foo", "foo")
    stage1 = run_copy("foo", "bar", single_stage=True)
    stage2 = run_copy("bar", "foobar", name="copy-bar-foobar")
    dvc.push()

    outs = ["foo", "bar", "foobar"]

    clean(outs, dvc)
    dvc.pull()
    assert all((tmp_dir / file).exists() for file in outs)

    for out, stage in zip(outs, [stage0, stage1, stage2]):
        for target in [stage.addressing, out]:
            clean(outs, dvc)
            stats = dvc.pull([target])
            assert stats["fetched"] == 1
            assert stats["added"] == [out]
            assert os.path.exists(out)
            assert not any(os.path.exists(out) for out in set(outs) - {out})

    clean(outs, dvc)
    stats = dvc.pull([stage2.addressing], with_deps=True)
    assert len(stats["added"]) == 3
    assert set(stats["added"]) == set(outs)

    clean(outs, dvc)
    stats = dvc.pull([os.curdir], recursive=True)
    assert set(stats["added"]) == set(outs)


def test_pipeline_file_target_ops(tmp_dir, dvc, run_copy, local_remote):
    path = local_remote.url
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", single_stage=True)

    tmp_dir.dvc_gen("lorem", "lorem")
    run_copy("lorem", "lorem2", name="copy-lorem-lorem2")

    tmp_dir.dvc_gen("ipsum", "ipsum")
    run_copy("ipsum", "baz", name="copy-ipsum-baz")

    outs = ["foo", "bar", "lorem", "ipsum", "baz", "lorem2"]

    remove(dvc.stage_cache.cache_dir)

    dvc.push()

    outs = ["foo", "bar", "lorem", "ipsum", "baz", "lorem2"]

    # each one's a copy of other, hence 3
    assert len(recurse_list_dir(path)) == 3

    clean(outs, dvc)
    assert set(dvc.pull(["dvc.yaml"])["added"]) == {"lorem2", "baz"}

    clean(outs, dvc)
    assert set(dvc.pull()["added"]) == set(outs)

    # clean everything in remote and push
    from dvc.testing.tmp_dir import TmpDir

    clean(TmpDir(path).iterdir())
    dvc.push(["dvc.yaml:copy-ipsum-baz"])
    assert len(recurse_list_dir(path)) == 1

    clean(TmpDir(path).iterdir())
    dvc.push(["dvc.yaml"])
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
    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})
    dvc.push()
    clean(["foo", "bar"], dvc)
    (tmp_dir / "bar").write_text("foobar")

    assert main(["pull", "--force"]) == 0

    out, _ = capsys.readouterr()
    assert "M\tbar".expandtabs() in out
    assert "A\tfoo".expandtabs() in out
    assert "2 files fetched" in out
    assert "1 file added" in out
    assert "1 file modified" in out

    main(["pull"])
    out, _ = capsys.readouterr()
    assert "Everything is up to date." in out


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
    assert dvc.pull(**{key: True})["fetched"] == expected


def test_push_pull_fetch_pipeline_stages(tmp_dir, dvc, run_copy, local_remote):
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", no_commit=True, name="copy-foo-bar")

    dvc.push("copy-foo-bar")
    assert len(recurse_list_dir(local_remote.url)) == 1
    # pushing everything so as we can check pull/fetch only downloads
    # from specified targets
    dvc.push()
    clean(["foo", "bar"], dvc)

    dvc.pull("copy-foo-bar")
    assert (tmp_dir / "bar").exists()
    assert len(recurse_list_dir(dvc.odb.local.cache_dir)) == 1
    clean(["bar"], dvc)

    dvc.fetch("copy-foo-bar")
    assert len(recurse_list_dir(dvc.odb.local.cache_dir)) == 1


def test_pull_partial(tmp_dir, dvc, local_remote):
    tmp_dir.dvc_gen({"foo": {"bar": {"baz": "baz"}, "spam": "spam"}})
    dvc.push()
    clean(["foo"], dvc)

    stats = dvc.pull(os.path.join("foo", "bar"))
    assert stats["fetched"] == 1
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

    dvc.pull()

    assert set(dvc.odb.local.all()) == {
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

    dvc.pull(remote="myremote")

    assert set(dvc.odb.local.all()) == {
        "acbd18db4cc2f85cedef654fccc4a4d8",
        "f97c5d29941bfb1b2fdab0874906ab82",
        "6b18131dc289fd37006705affe961ef8.dir",
        "b8a9f715dbb64fd5c56e7783c6820a61",
    }

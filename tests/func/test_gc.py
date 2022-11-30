import logging
import os
import shutil

import pytest

from dvc.cli import main
from dvc.exceptions import CollectCacheError
from dvc.fs import LocalFileSystem
from dvc.utils.fs import remove
from dvc_data.hashfile.db.local import LocalHashFileDB


@pytest.fixture
def good_and_bad_cache(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    (stage,) = tmp_dir.dvc_gen(
        "data",
        {"sub": {"data_sub": "data_sub", "data": "data", "тест": "проверка"}},
    )
    raw_dir_hash = stage.outs[0].hash_info.as_raw().value
    odb = dvc.odb.local

    bad_cache = {raw_dir_hash}
    for i in ["123", "234", "345"]:
        odb.add_bytes(i, i.encode("utf8"))
        bad_cache.add(i)

    good_cache = {md5 for md5 in odb.all() if md5 not in bad_cache}
    return good_cache, bad_cache


def test_gc_api(dvc, good_and_bad_cache):
    dvc.gc(workspace=True)
    odb = dvc.odb.local
    good_cache, bad_cache = good_and_bad_cache
    assert set(odb.oids_exist([*good_cache, *bad_cache])) == good_cache


def test_gc_cli(dvc, good_and_bad_cache):
    assert main(["gc", "-wf"]) == 0
    odb = dvc.odb.local
    good_cache, bad_cache = good_and_bad_cache
    assert set(odb.oids_exist([*good_cache, *bad_cache])) == good_cache


def test_gc_branches_tags(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen("file", "v1.0", commit="v1.0")
    scm.tag("v1.0")

    with tmp_dir.branch("test", new=True):
        dvc.remove("file.dvc")
        tmp_dir.dvc_gen("file", "test", commit="test")

    dvc.remove("file.dvc")
    tmp_dir.dvc_gen("file", "trash", commit="trash")

    dvc.remove("file.dvc")
    tmp_dir.dvc_gen("file", "master", commit="trash")

    odb = dvc.odb.local
    assert len(list(odb.all())) == 4

    dvc.gc(all_tags=True, all_branches=True)
    assert len(list(odb.all())) == 3

    dvc.gc(all_tags=False, all_branches=True)
    assert len(list(odb.all())) == 2

    dvc.gc(all_tags=True, all_branches=False)
    assert len(list(odb.all())) == 1


def test_gc_multiple_dvc_repos(tmp_dir, scm, dvc, erepo_dir):
    tmp_dir.dvc_gen("only_in_first", "only in main repo")
    tmp_dir.dvc_gen("in_both", "in both repos")

    erepo_dir.dvc.odb.local.path = dvc.odb.local.path
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("in_both", "in both repos")
        erepo_dir.dvc_gen("only_in_second", "only in additional repo")

    odb = dvc.odb.local
    assert len(list(odb.all())) == 3

    dvc.gc(repos=[erepo_dir], workspace=True)
    assert len(list(odb.all())) == 3

    dvc.gc(workspace=True)
    assert len(list(odb.all())) == 2


def test_all_commits(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("testfile", "uncommitted")
    tmp_dir.dvc_gen("testfile", "committed", commit="committed")
    tmp_dir.dvc_gen("testfile", "modified", commit="modified")
    tmp_dir.dvc_gen("testfile", "workspace")

    n = _count_files(dvc.odb.local.path)
    dvc.gc(all_commits=True)

    # Only one uncommitted file should go away
    assert _count_files(dvc.odb.local.path) == n - 1


def test_gc_no_dir_cache(tmp_dir, dvc):
    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})
    (dir_stage,) = tmp_dir.dvc_gen({"dir": {"x": "x", "subdir": {"y": "y"}}})

    remove(dir_stage.outs[0].cache_path)

    with pytest.raises(CollectCacheError):
        dvc.gc(workspace=True)

    assert _count_files(dvc.odb.local.path) == 4
    dvc.gc(force=True, workspace=True)
    assert _count_files(dvc.odb.local.path) == 2


def _count_files(path):
    return sum(len(files) for _, _, files in os.walk(path))


def test_gc_no_unpacked_dir(tmp_dir, dvc):
    dir_stages = tmp_dir.dvc_gen({"dir": {"file": "text"}})
    dvc.status()

    os.remove("dir.dvc")
    unpackeddir = (
        dir_stages[0].outs[0].cache_path + LocalHashFileDB.UNPACKED_DIR_SUFFIX
    )

    # older (pre 1.0) versions of dvc used to generate this dir
    shutil.copytree("dir", unpackeddir)
    assert os.path.exists(unpackeddir)

    dvc.gc(force=True, workspace=True)
    assert not os.path.exists(unpackeddir)


def test_gc_without_workspace_raises_error(tmp_dir, dvc):
    dvc.gc(force=True, workspace=True)  # works without error

    from dvc.exceptions import InvalidArgumentError

    with pytest.raises(InvalidArgumentError):
        dvc.gc(force=True)

    with pytest.raises(InvalidArgumentError):
        dvc.gc(force=True, workspace=False)


def test_gc_cloud_with_or_without_specifier(tmp_dir, erepo_dir, local_cloud):
    erepo_dir.add_remote(config=local_cloud.config)
    dvc = erepo_dir.dvc
    from dvc.exceptions import InvalidArgumentError

    with pytest.raises(InvalidArgumentError):
        dvc.gc(force=True, cloud=True)

    dvc.gc(cloud=True, all_tags=True)
    dvc.gc(cloud=True, all_commits=True)
    dvc.gc(cloud=True, all_branches=True)
    dvc.gc(cloud=True, all_commits=False, all_branches=True, all_tags=True)


def test_gc_without_workspace_on_tags_branches_commits(tmp_dir, dvc):
    dvc.gc(force=True, all_tags=True)
    dvc.gc(force=True, all_commits=True)
    dvc.gc(force=False, all_branches=True)

    # even if workspace is disabled, and others are enabled, assume as if
    # workspace is enabled.
    dvc.gc(force=False, all_branches=True, all_commits=False, workspace=False)


@pytest.mark.parametrize("cloud", ["c", ""])
def test_gc_without_workspace(tmp_dir, dvc, caplog, cloud):
    with caplog.at_level(logging.WARNING, logger="dvc"):
        assert main(["gc", f"-{cloud}vf"]) == 255

    assert (
        "Either of `-w|--workspace`, `-a|--all-branches`, `-T|--all-tags` "
        "`--all-experiments`, `--all-commits`, `--date` or `--rev` "
        "needs to be set."
    ) in caplog.text


def test_gc_with_possible_args_positive(tmp_dir, dvc):
    for flag in ["-w", "-a", "-T", "--all-commits", "-aT", "-wa", "-waT"]:
        assert main(["gc", "-vf", flag]) == 0


def test_gc_cloud_positive(tmp_dir, dvc, tmp_path_factory, local_remote):
    for flag in ["-cw", "-ca", "-cT", "-caT", "-cwT"]:
        assert main(["gc", "-vf", flag]) == 0


def test_gc_cloud_remove_order(tmp_dir, scm, dvc, tmp_path_factory, mocker):
    storage = os.fspath(tmp_path_factory.mktemp("test_remote_base"))
    dvc.config["remote"]["local_remote"] = {"url": storage}
    dvc.config["core"]["remote"] = "local_remote"

    (standalone, dir1, dir2) = tmp_dir.dvc_gen(
        {
            "file1": "standalone",
            "dir1": {"file2": "file2"},
            "dir2": {"file3": "file3", "file4": "file4"},
        }
    )
    dvc.push()
    dvc.remove(standalone.relpath)
    dvc.remove(dir1.relpath)
    dvc.remove(dir2.relpath)
    dvc.gc(workspace=True)

    mocked_remove = mocker.patch.object(
        LocalFileSystem, "remove", autospec=True
    )
    dvc.gc(workspace=True, cloud=True)
    assert len(mocked_remove.mock_calls) == 8
    # dir (and unpacked dir) should be first 4 checksums removed from
    # the remote
    for args in mocked_remove.call_args_list[:4]:
        checksum = str(args[0][1])
        assert checksum.endswith(".dir") or checksum.endswith(".dir.unpacked")


def test_gc_not_collect_pipeline_tracked_files(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import PIPELINE_FILE, Dvcfile

    tmp_dir.gen("foo", "foo")
    tmp_dir.gen("bar", "bar")

    run_copy("foo", "foo2", name="copy")
    shutil.rmtree(dvc.stage_cache.cache_dir)
    assert _count_files(dvc.odb.local.path) == 1
    dvc.gc(workspace=True, force=True)
    assert _count_files(dvc.odb.local.path) == 1

    # remove pipeline file and lockfile and check
    Dvcfile(dvc, PIPELINE_FILE).remove(force=True)
    dvc.gc(workspace=True, force=True)
    assert _count_files(dvc.odb.local.path) == 0


def test_gc_external_output(tmp_dir, dvc, workspace):
    workspace.gen({"foo": "foo", "bar": "bar"})

    (foo_stage,) = dvc.add("remote://workspace/foo")
    (bar_stage,) = dvc.add("remote://workspace/bar")

    foo_hash = foo_stage.outs[0].hash_info.value
    bar_hash = bar_stage.outs[0].hash_info.value

    assert (
        workspace / "cache" / foo_hash[:2] / foo_hash[2:]
    ).read_text() == "foo"
    assert (
        workspace / "cache" / bar_hash[:2] / bar_hash[2:]
    ).read_text() == "bar"

    (tmp_dir / "foo.dvc").unlink()

    dvc.gc(workspace=True)

    assert not (workspace / "cache" / foo_hash[:2] / foo_hash[2:]).exists()
    assert (
        workspace / "cache" / bar_hash[:2] / bar_hash[2:]
    ).read_text() == "bar"


def test_gc_all_experiments(tmp_dir, scm, dvc):
    from dvc.repo.experiments.refs import ExpRefInfo

    (foo,) = tmp_dir.dvc_gen("foo", "foo", commit="foo")
    foo_hash = foo.outs[0].hash_info.value

    (bar,) = tmp_dir.dvc_gen("foo", "bar", commit="bar")
    bar_hash = bar.outs[0].hash_info.value
    baseline = scm.get_rev()

    (baz,) = tmp_dir.dvc_gen("foo", "baz", commit="baz")
    baz_hash = baz.outs[0].hash_info.value

    ref = ExpRefInfo(baseline, "exp")
    scm.set_ref(str(ref), scm.get_rev())

    dvc.gc(all_experiments=True, force=True)

    # all_experiments will include the experiment commit (baz) plus baseline
    # commit (bar)
    assert not (
        tmp_dir / ".dvc" / "cache" / foo_hash[:2] / foo_hash[2:]
    ).exists()
    assert (
        tmp_dir / ".dvc" / "cache" / bar_hash[:2] / bar_hash[2:]
    ).read_text() == "bar"
    assert (
        tmp_dir / ".dvc" / "cache" / baz_hash[:2] / baz_hash[2:]
    ).read_text() == "baz"


def test_gc_rev_num(tmp_dir, scm, dvc):
    num = 2

    hashes = {}
    for i in range(4):
        i_str = str(i)
        f = tmp_dir.dvc_gen("foo", i_str, commit=i_str)
        hashes[i] = f[0].outs[0].hash_info.value

    dvc.gc(rev="HEAD", num=num, force=True)

    for n, i in enumerate(reversed(range(4))):
        cache = tmp_dir / ".dvc" / "cache" / hashes[i][:2] / hashes[i][2:]
        if n >= num:
            assert not cache.exists()
        else:
            assert cache.read_text() == str(i)

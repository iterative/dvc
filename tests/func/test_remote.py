import errno
import itertools
import os
import stat

import configobj
import pytest

from dvc.cli import main
from dvc.config import Config
from dvc.exceptions import DownloadError, UploadError
from dvc.utils.fs import remove


def test_remote(dvc):
    remotes = ["a", "b", "c"]

    assert main(["remote", "list"]) == 0
    assert main(["remote", "remove", remotes[0]]) != 0

    for r in remotes:
        assert main(["remote", "add", "--default", r, "s3://bucket/name"]) == 0

    assert main(["remote", "list"]) == 0

    assert main(["remote", "modify", remotes[0], "checksum_jobs", "1"]) == 0
    assert main(["remote", "remove", remotes[0]]) == 0

    assert main(["remote", "list"]) == 0


def test_remote_add_relative_path(dvc):
    dname = os.path.join("..", "path", "to", "dir")
    ret = main(["remote", "add", "mylocal", dname])
    assert ret == 0

    # NOTE: we are in the repo's root and config is in .dvc/, so
    # dir path written to config should be just one level above.
    rel = os.path.join("..", dname)
    config = configobj.ConfigObj(dvc.config.files["repo"])
    assert config['remote "mylocal"']["url"] == rel.replace("\\", "/")


def test_remote_overwrite(dvc):
    remote_name = "a"
    remote_url = "s3://bucket/name"
    assert main(["remote", "add", remote_name, remote_url]) == 0
    assert main(["remote", "add", remote_name, remote_url]) == 251
    assert main(["remote", "add", "-f", remote_name, remote_url]) == 0


def test_referencing_other_remotes(dvc):
    assert main(["remote", "add", "foo", "ssh://localhost/"]) == 0
    assert main(["remote", "add", "bar", "remote://foo/dvc-storage"]) == 0

    config = configobj.ConfigObj(dvc.config.files["repo"])
    assert config['remote "bar"']["url"] == "remote://foo/dvc-storage"


def test_remove_default(tmp_dir, dvc):
    remote = "mys3"
    assert main(["remote", "add", "--default", remote, "s3://bucket/name"]) == 0
    assert main(["remote", "modify", remote, "profile", "default"]) == 0
    assert main(["config", "--local", "core.remote", remote]) == 0

    config = configobj.ConfigObj(dvc.config.files["repo"])
    local_config = configobj.ConfigObj(dvc.config.files["local"])
    assert config["core"]["remote"] == remote
    assert local_config["core"]["remote"] == remote

    assert main(["remote", "remove", remote]) == 0

    config = configobj.ConfigObj(dvc.config.files["repo"])
    local_config = configobj.ConfigObj(dvc.config.files["local"])
    assert config.get("core", {}).get("remote") is None
    assert local_config.get("core", {}).get("remote") is None


def test_remote_remove(dvc):
    ret = main(["config", "core.checksum_jobs", "1"])
    assert ret == 0

    remote = "mys3"
    ret = main(["remote", "add", remote, "s3://bucket/name"])
    assert ret == 0

    ret = main(["remote", "remove", remote])
    assert ret == 0


def test_remote_default_cmd(dvc):
    remote = "mys3"
    ret = main(["remote", "add", "mys3", "s3://bucket/path"])
    assert ret == 0

    ret = main(["remote", "default", "mys3"])
    assert ret == 0
    config_file = os.path.join(dvc.dvc_dir, Config.CONFIG)
    config = configobj.ConfigObj(config_file)
    default = config["core"]["remote"]
    assert default == remote

    ret = main(["remote", "default", "--unset"])
    assert ret == 0
    config = configobj.ConfigObj(config_file)
    default = config.get("core", {}).get("remote")
    assert default is None


def test_show_default(dvc, capsys):
    assert main(["remote", "add", "foo", "s3://bucket/name"]) == 0
    assert main(["remote", "default", "foo"]) == 0
    assert main(["remote", "default"]) == 0
    out, _ = capsys.readouterr()
    assert out == "foo\n"


def test_upper_case_remote(tmp_dir, dvc, local_cloud):
    remote_name = "UPPERCASEREMOTE"

    tmp_dir.gen("foo", "foo")

    ret = main(["remote", "add", remote_name, local_cloud.url])
    assert ret == 0

    ret = main(["push", "-r", remote_name])
    assert ret == 0


def test_dir_hash_should_be_key_order_agnostic(tmp_dir, dvc, mocker):
    from dvc_data.hashfile.build import build
    from dvc_data.hashfile.tree import Tree

    tmp_dir.gen({"data": {"1": "1 content", "2": "2 content"}})

    path = (tmp_dir / "data").fs_path

    tree = Tree.from_list([{"relpath": "1", "md5": "1"}, {"relpath": "2", "md5": "2"}])
    tree.digest()

    mocker.patch("dvc_data.hashfile.build._build_tree", return_value=(None, tree))

    _, _, obj = build(dvc.cache.local, path, dvc.cache.local.fs, "md5")
    hash1 = obj.hash_info

    # remove the raw dir obj to force building the tree on the next build call
    dvc.cache.local.fs.remove(dvc.cache.local.oid_to_path(hash1.as_raw().value))

    tree = Tree.from_list([{"md5": "1", "relpath": "1"}, {"md5": "2", "relpath": "2"}])
    tree.digest()

    # NOTE: _build_tree already patched above
    _, _, obj = build(dvc.cache.local, path, dvc.cache.local.fs, "md5")
    hash2 = obj.hash_info

    assert hash1 == hash2


def test_partial_push_n_pull(  # noqa: C901
    tmp_dir, dvc, tmp_path_factory, local_remote, mocker
):
    from dvc_objects.fs import generic

    foo = tmp_dir.dvc_gen({"foo": "foo content"})[0].outs[0]
    bar = tmp_dir.dvc_gen({"bar": "bar content"})[0].outs[0]
    baz = tmp_dir.dvc_gen({"baz": {"foo": "foo content"}})[0].outs[0]

    # Faulty upload version, failing on foo
    original = generic.transfer
    odb = dvc.cloud.get_remote_odb("upstream")

    def unreliable_upload(from_fs, from_info, to_fs, to_info, **kwargs):
        on_error = kwargs["on_error"]
        assert on_error
        if isinstance(from_info, str):
            from_info = [from_info]
        else:
            from_info = list(from_info)
        if isinstance(to_info, str):
            to_info = [to_info]
        else:
            to_info = list(to_info)
        for i in range(len(from_info) - 1, -1, -1):
            from_i = from_info[i]
            to_i = to_info[i]
            if os.path.abspath(to_i) == os.path.abspath(
                odb.get(foo.hash_info.value).path
            ):
                if on_error:
                    on_error(from_i, to_i, Exception("stop foo"))
                del from_info[i]
                del to_info[i]
        return original(from_fs, from_info, to_fs, to_info, **kwargs)

    mock_upload = mocker.patch.object(generic, "transfer", unreliable_upload)
    with pytest.raises(UploadError) as upload_error_info:
        dvc.push()
    assert upload_error_info.value.amount == 2

    assert not odb.exists(foo.hash_info.value)
    assert odb.exists(bar.hash_info.value)
    assert not odb.exists(baz.hash_info.value)
    mocker.stop(mock_upload)

    # Push everything and delete local cache
    dvc.push()
    dvc.cache.local.clear()

    baz._collect_used_dir_cache()

    def unreliable_download(_from_fs, from_info, _to_fs, to_info, **kwargs):
        on_error = kwargs["on_error"]
        assert on_error
        if isinstance(from_info, str):
            from_info = [from_info]
        if isinstance(to_info, str):
            to_info = [to_info]
        for from_i, to_i in zip(from_info, to_info):
            on_error(from_i, to_i, Exception())

    mock_download = mocker.patch.object(generic, "transfer", unreliable_download)
    with pytest.raises(DownloadError) as download_error_info:
        dvc.pull()
    # error count should be len(.dir + standalone file checksums)
    # since files inside dir are ignored if dir cache entry is missing
    assert download_error_info.value.amount == 2
    mocker.stop(mock_download)


def test_raise_on_too_many_open_files(
    tmp_dir, dvc, tmp_path_factory, mocker, local_remote
):
    tmp_dir.dvc_gen({"file": "file content"})

    mocker.patch(
        "dvc_objects.fs.generic.transfer",
        side_effect=OSError(errno.EMFILE, "Too many open files"),
    )

    with pytest.raises(OSError, match="Too many open files") as e:
        dvc.push()
    assert e.value.errno == errno.EMFILE


def test_modify_missing_remote(tmp_dir, dvc):
    assert main(["remote", "modify", "myremote", "user", "xxx"]) == 251


def test_remote_modify_local_on_repo_config(tmp_dir, dvc):
    assert main(["remote", "add", "myremote", "http://example.com/path"]) == 0
    assert main(["remote", "modify", "myremote", "user", "xxx", "--local"]) == 0
    assert dvc.config.load_one("local")["remote"]["myremote"] == {"user": "xxx"}
    assert dvc.config.load_one("repo")["remote"]["myremote"] == {
        "url": "http://example.com/path"
    }
    dvc.config.load()
    assert dvc.config["remote"]["myremote"] == {
        "url": "http://example.com/path",
        "user": "xxx",
        "verify": False,
    }


def test_push_order(tmp_dir, dvc, tmp_path_factory, mocker, local_remote):
    from dvc_objects.fs import generic

    foo = tmp_dir.dvc_gen({"foo": {"bar": "bar content"}})[0].outs[0]
    tmp_dir.dvc_gen({"baz": "baz content"})

    mocked_upload = mocker.spy(generic, "transfer")
    dvc.push()

    # foo .dir file should be uploaded after bar
    odb = dvc.cloud.get_remote_odb("upstream")
    foo_path = odb.oid_to_path(foo.hash_info.value)
    bar_path = odb.oid_to_path(foo.obj._trie[("bar",)][1].value)
    paths = list(
        itertools.chain.from_iterable(
            args[3] for args, _ in mocked_upload.call_args_list
        )
    )
    assert paths.index(foo_path) > paths.index(bar_path)


def test_remote_modify_validation(dvc):
    remote_name = "drive"
    unsupported_config = "unsupported_config"
    assert main(["remote", "add", "-d", remote_name, "gdrive://test/test"]) == 0
    assert (
        main(["remote", "modify", remote_name, unsupported_config, "something"]) == 251
    )
    config = configobj.ConfigObj(dvc.config.files["repo"])
    assert unsupported_config not in config[f'remote "{remote_name}"']


def test_remote_modify_unset(dvc):
    assert main(["remote", "add", "-d", "myremote", "gdrive://test/test"]) == 0
    config = configobj.ConfigObj(dvc.config.files["repo"])
    assert config['remote "myremote"'] == {"url": "gdrive://test/test"}

    assert main(["remote", "modify", "myremote", "gdrive_client_id", "something"]) == 0
    config = configobj.ConfigObj(dvc.config.files["repo"])
    assert config['remote "myremote"'] == {
        "url": "gdrive://test/test",
        "gdrive_client_id": "something",
    }

    assert main(["remote", "modify", "myremote", "gdrive_client_id", "--unset"]) == 0
    config = configobj.ConfigObj(dvc.config.files["repo"])
    assert config['remote "myremote"'] == {"url": "gdrive://test/test"}


def test_remote_modify_default(dvc):
    remote_repo = "repo_level"
    remote_local = "local_level"
    wrong_name = "anything"
    assert main(["remote", "add", remote_repo, "s3://bucket/repo"]) == 0
    assert main(["remote", "add", remote_local, "s3://bucket/local"]) == 0

    assert main(["remote", "default", wrong_name]) == 251
    assert main(["remote", "default", remote_repo]) == 0
    assert main(["remote", "default", "--local", remote_local]) == 0

    repo_config = configobj.ConfigObj(dvc.config.files["repo"])
    local_config = configobj.ConfigObj(dvc.config.files["local"])

    assert repo_config["core"]["remote"] == remote_repo
    assert local_config["core"]["remote"] == remote_local


def test_remote_rename(dvc):
    remote_name = "drive"
    remote_url = "gdrive://test/test"
    new_name = "new"
    other_name = "other"
    # prepare
    assert main(["remote", "add", remote_name, remote_url]) == 0
    config = dvc.config.load_one("repo")
    assert config["remote"][remote_name]["url"] == remote_url
    assert new_name not in config.get("remote", {})

    # rename failed
    assert main(["remote", "rename", remote_name]) == 254
    assert main(["remote", "rename", new_name, other_name]) == 251
    config = dvc.config.load_one("repo")
    assert config["remote"][remote_name]["url"] == remote_url
    assert new_name not in config.get("remote", {})

    # rename success
    assert main(["remote", "rename", remote_name, new_name]) == 0
    config = dvc.config.load_one("repo")
    assert remote_name not in config.get("remote", {})
    assert config["remote"][new_name]["url"] == remote_url


def test_remote_duplicated(dvc):
    remote_name = "drive"
    remote_url = "gdrive://test/test"
    used_name = "overlap"
    another_url = "gdrive://test/test1"
    # prepare
    assert main(["remote", "add", remote_name, remote_url]) == 0
    assert main(["remote", "add", "--local", used_name, another_url]) == 0
    config = dvc.config.load_one("repo")
    assert config["remote"][remote_name]["url"] == remote_url
    local_config = dvc.config.load_one("local")
    assert local_config["remote"][used_name]["url"] == another_url

    # rename duplicated
    assert main(["remote", "rename", remote_name, used_name]) == 251
    config = dvc.config.load_one("repo")
    assert config["remote"][remote_name]["url"] == remote_url
    local_config = dvc.config.load_one("local")
    assert local_config["remote"][used_name]["url"] == another_url


def test_remote_default(dvc):
    remote_name = "drive"
    remote_url = "gdrive://test/test"
    new_name = "new"
    # prepare
    assert main(["remote", "add", "-d", remote_name, remote_url]) == 0
    assert main(["remote", "default", "--local", remote_name]) == 0
    config = dvc.config.load_one("repo")
    assert config["core"]["remote"] == remote_name
    assert config["remote"][remote_name]["url"] == remote_url
    assert new_name not in config.get("remote", {})
    local_config = dvc.config.load_one("local")
    assert local_config["core"]["remote"] == remote_name

    # rename success
    assert main(["remote", "rename", remote_name, new_name]) == 0
    config = dvc.config.load_one("repo")
    assert remote_name not in config.get("remote", {})
    assert config["core"]["remote"] == new_name
    assert config["remote"][new_name]["url"] == remote_url
    assert remote_name not in config.get("remote", {})
    local_config = dvc.config.load_one("local")
    assert local_config["core"]["remote"] == new_name


def test_protect_local_remote(tmp_dir, dvc, local_remote):
    (stage,) = tmp_dir.dvc_gen("file", "file content")

    dvc.push()
    odb = dvc.cloud.get_remote_odb("upstream")
    remote_cache_file = odb.oid_to_path(stage.outs[0].hash_info.value)

    assert os.path.exists(remote_cache_file)
    assert stat.S_IMODE(os.stat(remote_cache_file).st_mode) == 0o444


def test_push_incomplete_dir(tmp_dir, dvc, mocker, local_remote):
    (stage,) = tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    remote_odb = dvc.cloud.get_remote_odb("upstream")

    odb = dvc.cache.local
    out = stage.outs[0]
    file_objs = [entry_obj for _, _, entry_obj in out.obj]

    # remove one of the cache files for directory
    remove(odb.oid_to_path(file_objs[0].value))

    dvc.push()
    assert not remote_odb.exists(out.hash_info.value)
    assert not remote_odb.exists(file_objs[0].value)
    assert remote_odb.exists(file_objs[1].value)

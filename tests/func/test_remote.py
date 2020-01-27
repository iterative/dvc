import errno
import os
import shutil

import configobj
import pytest
from mock import patch

from dvc.config import Config, ConfigError
from dvc.exceptions import DownloadError, UploadError
from dvc.main import main
from dvc.path_info import PathInfo
from dvc.remote import RemoteLOCAL, RemoteConfig
from dvc.remote.base import RemoteBASE, RemoteCacheRequiredError
from dvc.compat import fspath
from tests.basic_env import TestDvc
from tests.remotes import Local


class TestRemote(TestDvc):
    def test(self):
        remotes = ["a", "b", "c"]

        self.assertEqual(main(["remote", "list"]), 0)
        self.assertNotEqual(main(["remote", "remove", remotes[0]]), 0)

        for r in remotes:
            self.assertEqual(
                main(["remote", "add", "--default", r, "s3://bucket/name"]), 0
            )

        self.assertEqual(main(["remote", "list"]), 0)

        self.assertEqual(
            main(["remote", "modify", remotes[0], "checksum_jobs", "1"]), 0
        )
        self.assertEqual(main(["remote", "remove", remotes[0]]), 0)

        self.assertEqual(main(["remote", "list"]), 0)

    def test_relative_path(self):
        dname = os.path.join("..", "path", "to", "dir")
        ret = main(["remote", "add", "mylocal", dname])
        self.assertEqual(ret, 0)

        # NOTE: we are in the repo's root and config is in .dvc/, so
        # dir path written to config should be just one level above.
        rel = os.path.join("..", dname)
        config = configobj.ConfigObj(self.dvc.config.config_file)
        self.assertEqual(config['remote "mylocal"']["url"], rel)

    def test_overwrite(self):
        remote_name = "a"
        remote_url = "s3://bucket/name"
        self.assertEqual(main(["remote", "add", remote_name, remote_url]), 0)
        self.assertEqual(main(["remote", "add", remote_name, remote_url]), 251)
        self.assertEqual(
            main(["remote", "add", "-f", remote_name, remote_url]), 0
        )

    def test_referencing_other_remotes(self):
        assert main(["remote", "add", "foo", "ssh://localhost/"]) == 0
        assert main(["remote", "add", "bar", "remote://foo/dvc-storage"]) == 0

        config = configobj.ConfigObj(self.dvc.config.config_file)

        assert config['remote "bar"']["url"] == "remote://foo/dvc-storage"


class TestRemoteRemoveDefault(TestDvc):
    def test(self):
        remote = "mys3"
        self.assertEqual(
            main(["remote", "add", "--default", remote, "s3://bucket/name"]), 0
        )
        self.assertEqual(
            main(["remote", "modify", remote, "profile", "default"]), 0
        )
        self.assertEqual(main(["config", "--local", "core.remote", remote]), 0)

        config = configobj.ConfigObj(
            os.path.join(self.dvc.dvc_dir, Config.CONFIG)
        )
        local_config = configobj.ConfigObj(
            os.path.join(self.dvc.dvc_dir, Config.CONFIG_LOCAL)
        )
        self.assertEqual(
            config[Config.SECTION_CORE][Config.SECTION_CORE_REMOTE], remote
        )
        self.assertEqual(
            local_config[Config.SECTION_CORE][Config.SECTION_CORE_REMOTE],
            remote,
        )

        self.assertEqual(main(["remote", "remove", remote]), 0)
        config = configobj.ConfigObj(
            os.path.join(self.dvc.dvc_dir, Config.CONFIG)
        )
        local_config = configobj.ConfigObj(
            os.path.join(self.dvc.dvc_dir, Config.CONFIG_LOCAL)
        )
        section = Config.SECTION_REMOTE_FMT.format(remote)
        self.assertTrue(section not in config.keys())

        core = config.get(Config.SECTION_CORE, None)
        if core is not None:
            self.assertTrue(Config.SECTION_CORE_REMOTE not in core.keys())

        core = local_config.get(Config.SECTION_CORE, None)
        if core is not None:
            self.assertTrue(Config.SECTION_CORE_REMOTE not in core.keys())


class TestRemoteRemove(TestDvc):
    def test(self):
        ret = main(["config", "core.checksum_jobs", "1"])
        self.assertEqual(ret, 0)

        remote = "mys3"
        ret = main(["remote", "add", remote, "s3://bucket/name"])
        self.assertEqual(ret, 0)

        ret = main(["remote", "remove", remote])
        self.assertEqual(ret, 0)


class TestRemoteDefault(TestDvc):
    def test(self):
        remote = "mys3"
        ret = main(["remote", "add", "mys3", "s3://bucket/path"])
        self.assertEqual(ret, 0)

        ret = main(["remote", "default", "mys3"])
        self.assertEqual(ret, 0)
        config_file = os.path.join(self.dvc.dvc_dir, Config.CONFIG)
        config = configobj.ConfigObj(config_file)
        default = config[Config.SECTION_CORE][Config.SECTION_CORE_REMOTE]
        self.assertEqual(default, remote)

        ret = main(["remote", "default", "--unset"])
        self.assertEqual(ret, 0)
        config = configobj.ConfigObj(config_file)
        core = config.get(Config.SECTION_CORE)
        if core is not None:
            default = core.get(Config.SECTION_CORE_REMOTE)
        else:
            default = None
        self.assertEqual(default, None)


def test_show_default(dvc, capsys):
    assert main(["remote", "default", "foo"]) == 0
    assert main(["remote", "default"]) == 0
    out, _ = capsys.readouterr()
    assert out == "foo\n"


class TestRemoteShouldHandleUppercaseRemoteName(TestDvc):
    upper_case_remote_name = "UPPERCASEREMOTE"

    def test(self):
        remote_url = Local.get_storagepath()
        ret = main(["remote", "add", self.upper_case_remote_name, remote_url])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["push", "-r", self.upper_case_remote_name])
        self.assertEqual(ret, 0)


def test_dir_checksum_should_be_key_order_agnostic(tmp_dir, dvc):
    tmp_dir.gen({"data": {"1": "1 content", "2": "2 content"}})

    path_info = PathInfo("data")
    with dvc.state:
        with patch.object(
            RemoteBASE,
            "_collect_dir",
            return_value=[
                {"relpath": "1", "md5": "1"},
                {"relpath": "2", "md5": "2"},
            ],
        ):
            checksum1 = dvc.cache.local.get_dir_checksum(path_info)

        with patch.object(
            RemoteBASE,
            "_collect_dir",
            return_value=[
                {"md5": "1", "relpath": "1"},
                {"md5": "2", "relpath": "2"},
            ],
        ):
            checksum2 = dvc.cache.local.get_dir_checksum(path_info)

    assert checksum1 == checksum2


def test_partial_push_n_pull(tmp_dir, dvc, tmp_path_factory):
    remote_config = RemoteConfig(dvc.config)
    remote_config.add(
        "upstream", fspath(tmp_path_factory.mktemp("upstream")), default=True
    )

    foo = tmp_dir.dvc_gen({"foo": "foo content"})[0].outs[0]
    bar = tmp_dir.dvc_gen({"bar": "bar content"})[0].outs[0]

    # Faulty upload version, failing on foo
    original = RemoteLOCAL._upload

    def unreliable_upload(self, from_file, to_info, name=None, **kwargs):
        if name == "foo":
            raise Exception("stop foo")
        return original(self, from_file, to_info, name, **kwargs)

    with patch.object(RemoteLOCAL, "_upload", unreliable_upload):
        with pytest.raises(UploadError) as upload_error_info:
            dvc.push()
        assert upload_error_info.value.amount == 1

        remote = dvc.cloud.get_remote("upstream")
        assert not remote.exists(remote.checksum_to_path_info(foo.checksum))
        assert remote.exists(remote.checksum_to_path_info(bar.checksum))

    # Push everything and delete local cache
    dvc.push()
    shutil.rmtree(dvc.cache.local.cache_dir)

    with patch.object(RemoteLOCAL, "_download", side_effect=Exception):
        with pytest.raises(DownloadError) as download_error_info:
            dvc.pull()
        assert download_error_info.value.amount == 2


def test_raise_on_too_many_open_files(tmp_dir, dvc, tmp_path_factory, mocker):
    storage = tmp_path_factory.mktemp("test_remote_base")
    remote_config = RemoteConfig(dvc.config)
    remote_config.add("local_remote", fspath(storage), default=True)

    tmp_dir.dvc_gen({"file": "file content"})

    mocker.patch.object(
        RemoteLOCAL,
        "_upload",
        side_effect=OSError(errno.EMFILE, "Too many open files"),
    )

    with pytest.raises(OSError) as e:
        dvc.push()
        assert e.errno == errno.EMFILE


def test_modify_missing_remote(dvc):
    remote_config = RemoteConfig(dvc.config)

    with pytest.raises(ConfigError, match=r"Unable to find remote section"):
        remote_config.modify("myremote", "gdrive_client_id", "xxx")


def test_external_dir_resource_on_no_cache(tmp_dir, dvc, tmp_path_factory):
    # https://github.com/iterative/dvc/issues/2647, is some situations
    # (external dir dependency) cache is required to calculate dir md5
    external_dir = tmp_path_factory.mktemp("external_dir")
    (external_dir / "file").write_text("content")

    dvc.cache.local = None
    with pytest.raises(RemoteCacheRequiredError):
        dvc.run(deps=[fspath(external_dir)])

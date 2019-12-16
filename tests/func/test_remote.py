import errno
import os
import shutil

import configobj
import pytest
from mock import patch

from dvc.config import Config
from dvc.main import main
from dvc.path_info import PathInfo
from dvc.remote import RemoteLOCAL, RemoteConfig
from dvc.remote.base import RemoteBASE
from dvc.utils.compat import fspath
from tests.basic_env import TestDvc
from tests.remotes import get_local_url, get_local_storagepath


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

        self.assertEqual(main(["remote", "remove", remotes[0]]), 0)
        self.assertEqual(
            main(["remote", "modify", remotes[0], "option", "value"]), 0
        )

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
        ret = main(["config", "core.jobs", "1"])
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


def test_show_default(dvc_repo, capsys):
    assert main(["remote", "default", "foo"]) == 0
    assert main(["remote", "default"]) == 0
    out, _ = capsys.readouterr()
    assert out == "foo\n"


class TestRemoteShouldHandleUppercaseRemoteName(TestDvc):
    upper_case_remote_name = "UPPERCASEREMOTE"

    def test(self):
        remote_url = get_local_storagepath()
        ret = main(["remote", "add", self.upper_case_remote_name, remote_url])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["push", "-r", self.upper_case_remote_name])
        self.assertEqual(ret, 0)


def test_large_dir_progress(repo_dir, dvc_repo):
    from dvc.utils import LARGE_DIR_SIZE
    from dvc.progress import Tqdm

    # Create a "large dir"
    for i in range(LARGE_DIR_SIZE + 1):
        repo_dir.create(os.path.join("gen", "{}.txt".format(i)), str(i))

    with patch.object(Tqdm, "update") as update:
        assert not update.called
        dvc_repo.add("gen")
        assert update.called


def test_dir_checksum_should_be_key_order_agnostic(dvc_repo):
    data_dir = os.path.join(dvc_repo.root_dir, "data")
    file1 = os.path.join(data_dir, "1")
    file2 = os.path.join(data_dir, "2")

    os.mkdir(data_dir)
    with open(file1, "w") as fobj:
        fobj.write("1")

    with open(file2, "w") as fobj:
        fobj.write("2")

    path_info = PathInfo(data_dir)
    with dvc_repo.state:
        with patch.object(
            RemoteBASE,
            "_collect_dir",
            return_value=[
                {"relpath": "1", "md5": "1"},
                {"relpath": "2", "md5": "2"},
            ],
        ):
            checksum1 = dvc_repo.cache.local.get_dir_checksum(path_info)

        with patch.object(
            RemoteBASE,
            "_collect_dir",
            return_value=[
                {"md5": "1", "relpath": "1"},
                {"md5": "2", "relpath": "2"},
            ],
        ):
            checksum2 = dvc_repo.cache.local.get_dir_checksum(path_info)

    assert checksum1 == checksum2


def test_partial_push_n_pull(dvc_repo, repo_dir, caplog):
    assert main(["remote", "add", "-d", "upstream", get_local_url()]) == 0
    # Recreate the repo to reread config
    repo = dvc_repo.__class__(dvc_repo.root_dir)
    remote = repo.cloud.get_remote("upstream")

    foo = repo.add(repo_dir.FOO)[0].outs[0]
    bar = repo.add(repo_dir.BAR)[0].outs[0]

    # Faulty upload version, failing on foo
    original = RemoteLOCAL._upload

    def unreliable_upload(self, from_file, to_info, name=None, **kwargs):
        if name == "foo":
            raise Exception("stop foo")
        return original(self, from_file, to_info, name, **kwargs)

    with patch.object(RemoteLOCAL, "_upload", unreliable_upload):
        assert main(["push"]) == 1
        assert str(get_last_exc(caplog)) == "1 files failed to upload"

        assert not remote.exists(remote.checksum_to_path_info(foo.checksum))
        assert remote.exists(remote.checksum_to_path_info(bar.checksum))

    # Push everything and delete local cache
    assert main(["push"]) == 0
    shutil.rmtree(repo.cache.local.cache_dir)

    with patch.object(RemoteLOCAL, "_download", side_effect=Exception):
        assert main(["pull"]) == 1
        assert str(get_last_exc(caplog)) == "2 files failed to download"


def get_last_exc(caplog):
    _, exc, _ = caplog.records[-2].exc_info
    return exc


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

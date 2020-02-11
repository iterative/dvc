import copy
import logging
import os
import shutil
import uuid
from unittest import SkipTest

import pytest

from dvc.cache import NamedCache
from dvc.compat import fspath
from dvc.config import Config
from dvc.data_cloud import DataCloud
from dvc.main import main
from dvc.remote import RemoteAZURE, RemoteConfig
from dvc.remote import RemoteGDrive
from dvc.remote import RemoteGS
from dvc.remote import RemoteHDFS
from dvc.remote import RemoteHTTP
from dvc.remote import RemoteLOCAL
from dvc.remote import RemoteOSS
from dvc.remote import RemoteS3
from dvc.remote import RemoteSSH
from dvc.remote.base import STATUS_DELETED
from dvc.remote.base import STATUS_NEW
from dvc.remote.base import STATUS_OK
from dvc.utils import file_md5
from dvc.utils.stage import dump_stage_file
from dvc.utils.stage import load_stage_file
from tests.basic_env import TestDvc

from tests.remotes import (
    Azure,
    GCP,
    GDrive,
    HDFS,
    Local,
    S3,
    SSHMocked,
    OSS,
    TEST_CONFIG,
    TEST_SECTION,
    TEST_GCP_CREDS_FILE,
    TEST_GDRIVE_CLIENT_ID,
    TEST_GDRIVE_CLIENT_SECRET,
    TEST_REMOTE,
)


class TestDataCloud(TestDvc):
    def _test_cloud(self, config, cl):
        self.dvc.config.config = config
        cloud = DataCloud(self.dvc)
        self.assertIsInstance(cloud.get_remote(), cl)

    def test(self):
        config = copy.deepcopy(TEST_CONFIG)

        clist = [
            ("s3://mybucket/", RemoteS3),
            ("gs://mybucket/", RemoteGS),
            ("ssh://user@localhost:/", RemoteSSH),
            ("http://localhost:8000/", RemoteHTTP),
            ("azure://ContainerName=mybucket;conn_string;", RemoteAZURE),
            ("oss://mybucket/", RemoteOSS),
            (TestDvc.mkdtemp(), RemoteLOCAL),
        ]

        for scheme, cl in clist:
            remote_url = scheme + str(uuid.uuid4())
            config[TEST_SECTION][Config.SECTION_REMOTE_URL] = remote_url
            self._test_cloud(config, cl)


class TestDataCloudBase(TestDvc):
    def _get_cloud_class(self):
        return None

    @staticmethod
    def should_test():
        return False

    @staticmethod
    def get_url():
        return NotImplementedError

    def _get_keyfile(self):
        return None

    def _ensure_should_run(self):
        if not self.should_test():
            raise SkipTest(
                "Test {} is disabled".format(self.__class__.__name__)
            )

    def _setup_cloud(self):
        self._ensure_should_run()

        repo = self.get_url()
        keyfile = self._get_keyfile()

        config = copy.deepcopy(TEST_CONFIG)
        config[TEST_SECTION][Config.SECTION_REMOTE_URL] = repo
        config[TEST_SECTION][Config.SECTION_REMOTE_KEY_FILE] = keyfile
        self.dvc.config.config = config
        self.cloud = DataCloud(self.dvc)

        self.assertIsInstance(self.cloud.get_remote(), self._get_cloud_class())

    def _test_cloud(self):
        self._setup_cloud()

        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)
        out = stage.outs[0]
        cache = out.cache_path
        md5 = out.checksum
        info = out.get_used_cache()

        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        stage_dir = stages[0]
        self.assertTrue(stage_dir is not None)
        out_dir = stage_dir.outs[0]
        cache_dir = out_dir.cache_path
        name_dir = str(out_dir)
        md5_dir = out_dir.checksum
        info_dir = NamedCache.make(out_dir.scheme, md5_dir, name_dir)

        with self.cloud.repo.state:
            # Check status
            status = self.cloud.status(info, show_checksums=True)
            expected = {md5: {"name": md5, "status": STATUS_NEW}}
            self.assertEqual(status, expected)

            status_dir = self.cloud.status(info_dir, show_checksums=True)
            expected = {md5_dir: {"name": md5_dir, "status": STATUS_NEW}}
            self.assertEqual(status_dir, expected)

            # Push and check status
            self.cloud.push(info)
            self.assertTrue(os.path.exists(cache))
            self.assertTrue(os.path.isfile(cache))

            self.cloud.push(info_dir)
            self.assertTrue(os.path.isfile(cache_dir))

            status = self.cloud.status(info, show_checksums=True)
            expected = {md5: {"name": md5, "status": STATUS_OK}}
            self.assertEqual(status, expected)

            status_dir = self.cloud.status(info_dir, show_checksums=True)
            expected = {md5_dir: {"name": md5_dir, "status": STATUS_OK}}
            self.assertEqual(status_dir, expected)

            # Remove and check status
            shutil.rmtree(self.dvc.cache.local.cache_dir)

            status = self.cloud.status(info, show_checksums=True)
            expected = {md5: {"name": md5, "status": STATUS_DELETED}}
            self.assertEqual(status, expected)

            status_dir = self.cloud.status(info_dir, show_checksums=True)
            expected = {md5_dir: {"name": md5_dir, "status": STATUS_DELETED}}
            self.assertEqual(status_dir, expected)

            # Pull and check status
            self.cloud.pull(info)
            self.assertTrue(os.path.exists(cache))
            self.assertTrue(os.path.isfile(cache))
            with open(cache, "r") as fd:
                self.assertEqual(fd.read(), self.FOO_CONTENTS)

            self.cloud.pull(info_dir)
            self.assertTrue(os.path.isfile(cache_dir))

            status = self.cloud.status(info, show_checksums=True)
            expected = {md5: {"name": md5, "status": STATUS_OK}}
            self.assertEqual(status, expected)

            status_dir = self.cloud.status(info_dir, show_checksums=True)
            expected = {md5_dir: {"name": md5_dir, "status": STATUS_OK}}
            self.assertTrue(status_dir, expected)

    def test(self):
        self._ensure_should_run()
        self._test_cloud()


class TestRemoteS3(S3, TestDataCloudBase):
    def _get_cloud_class(self):
        return RemoteS3


def setup_gdrive_cloud(remote_url, dvc):
    config = copy.deepcopy(TEST_CONFIG)
    config[TEST_SECTION][Config.SECTION_REMOTE_URL] = remote_url
    config[TEST_SECTION][
        Config.SECTION_GDRIVE_CLIENT_ID
    ] = TEST_GDRIVE_CLIENT_ID
    config[TEST_SECTION][
        Config.SECTION_GDRIVE_CLIENT_SECRET
    ] = TEST_GDRIVE_CLIENT_SECRET

    dvc.config.config = config
    remote = DataCloud(dvc).get_remote()
    remote._create_remote_dir("root", remote.path_info.path)


class TestRemoteGDrive(GDrive, TestDataCloudBase):
    def _setup_cloud(self):
        self._ensure_should_run()

        setup_gdrive_cloud(self.get_url(), self.dvc)

        self.cloud = DataCloud(self.dvc)
        remote = self.cloud.get_remote()
        self.assertIsInstance(remote, self._get_cloud_class())

    def _get_cloud_class(self):
        return RemoteGDrive


class TestRemoteGS(GCP, TestDataCloudBase):
    def _setup_cloud(self):
        self._ensure_should_run()

        repo = self.get_url()

        config = copy.deepcopy(TEST_CONFIG)
        config[TEST_SECTION][Config.SECTION_REMOTE_URL] = repo
        config[TEST_SECTION][
            Config.SECTION_GCP_CREDENTIALPATH
        ] = TEST_GCP_CREDS_FILE
        self.dvc.config.config = config
        self.cloud = DataCloud(self.dvc)

        self.assertIsInstance(self.cloud.get_remote(), self._get_cloud_class())

    def _get_cloud_class(self):
        return RemoteGS


class TestRemoteAZURE(Azure, TestDataCloudBase):
    def _get_cloud_class(self):
        return RemoteAZURE


class TestRemoteOSS(OSS, TestDataCloudBase):
    def _get_cloud_class(self):
        return RemoteOSS


class TestRemoteLOCAL(Local, TestDataCloudBase):
    def _get_cloud_class(self):
        return RemoteLOCAL


@pytest.mark.usefixtures("ssh_server")
class TestRemoteSSHMocked(SSHMocked, TestDataCloudBase):
    @pytest.fixture(autouse=True)
    def setup_method_fixture(self, request, ssh_server):
        self.ssh_server = ssh_server
        self.method_name = request.function.__name__

    def _setup_cloud(self):
        self._ensure_should_run()

        repo = self.get_url()
        keyfile = self._get_keyfile()

        config = copy.deepcopy(TEST_CONFIG)
        config[TEST_SECTION][Config.SECTION_REMOTE_URL] = repo
        config[TEST_SECTION][Config.SECTION_REMOTE_KEY_FILE] = keyfile
        config[TEST_SECTION][Config.SECTION_REMOTE_NO_TRAVERSE] = False
        self.dvc.config.config = config
        self.cloud = DataCloud(self.dvc)

        self.assertIsInstance(self.cloud.get_remote(), self._get_cloud_class())

    def get_url(self):
        user = self.ssh_server.test_creds["username"]
        return super().get_url(user, self.ssh_server.port)

    def _get_keyfile(self):
        return self.ssh_server.test_creds["key_filename"]

    def _get_cloud_class(self):
        return RemoteSSH


class TestRemoteHDFS(HDFS, TestDataCloudBase):
    def _get_cloud_class(self):
        return RemoteHDFS


class TestDataCloudCLIBase(TestDvc):
    def main(self, args):
        ret = main(args)
        self.assertEqual(ret, 0)

    @staticmethod
    def should_test():
        return False

    @staticmethod
    def get_url():
        raise NotImplementedError

    def _setup_cloud(self):
        pass

    def _test_cloud(self, remote=None):
        self._setup_cloud()

        args = ["-v", "-j", "2"]
        if remote:
            args += ["-r", remote]
        else:
            args += []

        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)
        cache = stage.outs[0].cache_path

        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        stage_dir = stages[0]
        self.assertTrue(stage_dir is not None)
        cache_dir = stage_dir.outs[0].cache_path

        # FIXME check status output

        self.main(["push"] + args)
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        self.assertTrue(os.path.isfile(cache_dir))

        shutil.rmtree(self.dvc.cache.local.cache_dir)

        self.main(["fetch"] + args)
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        self.assertTrue(os.path.isfile(cache_dir))

        self.main(["pull"] + args)
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        self.assertTrue(os.path.isfile(cache_dir))
        self.assertTrue(os.path.isfile(self.FOO))
        self.assertTrue(os.path.isdir(self.DATA_DIR))

        with open(cache, "r") as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)
        self.assertTrue(os.path.isfile(cache_dir))

        # NOTE: check if remote gc works correctly on directories
        self.main(["gc", "-c", "-f"] + args)
        shutil.move(
            self.dvc.cache.local.cache_dir,
            self.dvc.cache.local.cache_dir + ".back",
        )

        self.main(["fetch"] + args)

        self.main(["pull", "-f"] + args)
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        self.assertTrue(os.path.isfile(cache_dir))
        self.assertTrue(os.path.isfile(self.FOO))
        self.assertTrue(os.path.isdir(self.DATA_DIR))

    def _test(self):
        pass

    def test(self):
        if not self.should_test():
            raise SkipTest(
                "Test {} is disabled".format(self.__class__.__name__)
            )
        self._test()


class TestRemoteLOCALCLI(Local, TestDataCloudCLIBase):
    def _test(self):
        url = self.get_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteHDFSCLI(HDFS, TestDataCloudCLIBase):
    def _test(self):
        url = self.get_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteS3CLI(S3, TestDataCloudCLIBase):
    def _test(self):
        url = self.get_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteGDriveCLI(GDrive, TestDataCloudCLIBase):
    def _setup_cloud(self):
        setup_gdrive_cloud(self.get_url(), self.dvc)

    def _test(self):
        url = self.get_url()

        self.main(["remote", "add", TEST_REMOTE, url])
        self.main(
            [
                "remote",
                "modify",
                TEST_REMOTE,
                Config.SECTION_GDRIVE_CLIENT_ID,
                TEST_GDRIVE_CLIENT_ID,
            ]
        )
        self.main(
            [
                "remote",
                "modify",
                TEST_REMOTE,
                Config.SECTION_GDRIVE_CLIENT_SECRET,
                TEST_GDRIVE_CLIENT_SECRET,
            ]
        )

        self._test_cloud(TEST_REMOTE)


class TestRemoteGSCLI(GCP, TestDataCloudCLIBase):
    def _test(self):
        url = self.get_url()

        self.main(["remote", "add", TEST_REMOTE, url])
        self.main(
            [
                "remote",
                "modify",
                TEST_REMOTE,
                "credentialpath",
                TEST_GCP_CREDS_FILE,
            ]
        )

        self._test_cloud(TEST_REMOTE)


class TestRemoteAZURECLI(Azure, TestDataCloudCLIBase):
    def _test(self):
        url = self.get_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteOSSCLI(OSS, TestDataCloudCLIBase):
    def _test(self):
        url = self.get_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestDataCloudErrorCLI(TestDvc):
    def main_fail(self, args):
        ret = main(args)
        self.assertNotEqual(ret, 0)

    def test_error(self):
        f = "non-existing-file"
        self.main_fail(["status", "-c", f])
        self.main_fail(["push", f])
        self.main_fail(["pull", f])
        self.main_fail(["fetch", f])


class TestWarnOnOutdatedStage(TestDvc):
    def main(self, args):
        ret = main(args)
        self.assertEqual(ret, 0)

    def _test(self):
        url = Local.get_url()
        self.main(["remote", "add", "-d", TEST_REMOTE, url])

        stage = self.dvc.run(outs=["bar"], cmd="echo bar > bar")
        self.main(["push"])

        stage_file_path = stage.relpath
        content = load_stage_file(stage_file_path)
        del content["outs"][0]["md5"]
        dump_stage_file(stage_file_path, content)

        with self._caplog.at_level(logging.WARNING, logger="dvc"):
            self._caplog.clear()
            self.main(["status", "-c"])
            expected_warning = (
                "Output 'bar'(Stage: 'bar.dvc') is missing version info. "
                "Cache for it will not be collected. "
                "Use `dvc repro` to get your pipeline up to date."
            )

            assert expected_warning in self._caplog.text

    def test(self):
        self._test()


class TestRecursiveSyncOperations(Local, TestDataCloudBase):
    def main(self, args):
        ret = main(args)
        self.assertEqual(ret, 0)

    def _get_cloud_class(self):
        return RemoteLOCAL

    def _prepare_repo(self):
        remote = self.cloud.get_remote()
        self.main(["remote", "add", "-d", TEST_REMOTE, remote.cache_dir])

        self.dvc.add(self.DATA)
        self.dvc.add(self.DATA_SUB)

    def _remove_local_data_files(self):
        os.remove(self.DATA)
        os.remove(self.DATA_SUB)

    def _test_recursive_pull(self):
        self._remove_local_data_files()
        self._clear_local_cache()

        self.assertFalse(os.path.exists(self.DATA))
        self.assertFalse(os.path.exists(self.DATA_SUB))

        self.main(["pull", "-R", self.DATA_DIR])

        self.assertTrue(os.path.exists(self.DATA))
        self.assertTrue(os.path.exists(self.DATA_SUB))

    def _clear_local_cache(self):
        shutil.rmtree(self.dvc.cache.local.cache_dir)

    def _test_recursive_fetch(self, data_md5, data_sub_md5):
        self._clear_local_cache()

        local_cache_data_path = self.dvc.cache.local.get(data_md5)
        local_cache_data_sub_path = self.dvc.cache.local.get(data_sub_md5)

        self.assertFalse(os.path.exists(local_cache_data_path))
        self.assertFalse(os.path.exists(local_cache_data_sub_path))

        self.main(["fetch", "-R", self.DATA_DIR])

        self.assertTrue(os.path.exists(local_cache_data_path))
        self.assertTrue(os.path.exists(local_cache_data_sub_path))

    def _test_recursive_push(self, data_md5, data_sub_md5):
        remote = self.cloud.get_remote()
        cloud_data_path = remote.get(data_md5)
        cloud_data_sub_path = remote.get(data_sub_md5)

        self.assertFalse(os.path.exists(cloud_data_path))
        self.assertFalse(os.path.exists(cloud_data_sub_path))

        self.main(["push", "-R", self.DATA_DIR])

        self.assertTrue(os.path.exists(cloud_data_path))
        self.assertTrue(os.path.exists(cloud_data_sub_path))

    def test(self):
        self._setup_cloud()
        self._prepare_repo()

        data_md5 = file_md5(self.DATA)[0]
        data_sub_md5 = file_md5(self.DATA_SUB)[0]

        self._test_recursive_push(data_md5, data_sub_md5)

        self._test_recursive_fetch(data_md5, data_sub_md5)

        self._test_recursive_pull()


def test_checksum_recalculation(mocker, dvc, tmp_dir):
    tmp_dir.gen({"foo": "foo"})
    test_get_file_checksum = mocker.spy(RemoteLOCAL, "get_file_checksum")
    url = Local.get_url()
    ret = main(["remote", "add", "-d", TEST_REMOTE, url])
    assert ret == 0
    ret = main(["config", "cache.type", "hardlink"])
    assert ret == 0
    ret = main(["add", "foo"])
    assert ret == 0
    ret = main(["push"])
    assert ret == 0
    ret = main(["run", "-d", "foo", "echo foo"])
    assert ret == 0
    assert test_get_file_checksum.mock.call_count == 1


class TestShouldWarnOnNoChecksumInLocalAndRemoteCache(TestDvc):
    def setUp(self):
        super().setUp()

        cache_dir = self.mkdtemp()
        ret = main(["add", self.FOO])
        self.assertEqual(0, ret)

        ret = main(["add", self.BAR])
        self.assertEqual(0, ret)

        # purge cache
        shutil.rmtree(self.dvc.cache.local.cache_dir)

        ret = main(["remote", "add", "remote_name", "-d", cache_dir])
        self.assertEqual(0, ret)

        checksum_foo = file_md5(self.FOO)[0]
        checksum_bar = file_md5(self.BAR)[0]
        self.message_header = (
            "Some of the cache files do not exist neither locally "
            "nor on remote. Missing cache files: "
        )
        self.message_bar_part = "name: {}, md5: {}".format(
            self.BAR, checksum_bar
        )
        self.message_foo_part = "name: {}, md5: {}".format(
            self.FOO, checksum_foo
        )

    def test(self):
        self._caplog.clear()
        main(["push"])
        assert self.message_header in self._caplog.text
        assert self.message_foo_part in self._caplog.text
        assert self.message_bar_part in self._caplog.text

        self._caplog.clear()
        main(["pull"])
        assert self.message_header in self._caplog.text
        assert self.message_foo_part in self._caplog.text
        assert self.message_bar_part in self._caplog.text

        self._caplog.clear()
        main(["status", "-c"])
        assert self.message_header in self._caplog.text
        assert self.message_foo_part in self._caplog.text
        assert self.message_bar_part in self._caplog.text


def test_verify_checksums(tmp_dir, scm, dvc, mocker, tmp_path_factory):
    tmp_dir.dvc_gen({"file": "file1 content"}, commit="add file")
    tmp_dir.dvc_gen({"dir": {"subfile": "file2 content"}}, commit="add dir")

    RemoteConfig(dvc.config).add(
        "local_remote",
        fspath(tmp_path_factory.mktemp("local_remote")),
        default=True,
    )
    dvc.push()

    # remove artifacts and cache to trigger fetching
    os.remove("file")
    shutil.rmtree("dir")
    shutil.rmtree(dvc.cache.local.cache_dir)

    checksum_spy = mocker.spy(dvc.cache.local, "get_file_checksum")

    dvc.pull()
    assert checksum_spy.call_count == 0

    # Removing cache will invalidate existing state entries
    shutil.rmtree(dvc.cache.local.cache_dir)

    dvc.config.set(
        Config.SECTION_REMOTE_FMT.format("local_remote"),
        Config.SECTION_REMOTE_VERIFY,
        "True",
    )

    dvc.pull()
    assert checksum_spy.call_count == 3

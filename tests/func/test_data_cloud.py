import copy
import logging
import os
import shutil
import uuid
from unittest import SkipTest

import pytest

from dvc.compat import fspath, fspath_py35
from dvc.cache import NamedCache
from dvc.data_cloud import DataCloud
from dvc.main import main
from dvc.remote import AzureRemote
from dvc.remote import GDriveRemote
from dvc.remote import GSRemote
from dvc.remote import HDFSRemote
from dvc.remote import HTTPRemote
from dvc.remote import LocalRemote
from dvc.remote import OSSRemote
from dvc.remote import S3Remote
from dvc.remote import SSHRemote
from dvc.remote.base import STATUS_DELETED, STATUS_NEW, STATUS_OK
from dvc.stage.exceptions import StageNotFound
from dvc.utils import file_md5
from dvc.utils.fs import remove
from dvc.utils.stage import dump_stage_file, load_stage_file
from dvc.external_repo import clean_repos
from tests.basic_env import TestDvc

from tests.remotes import (
    Azure,
    GCP,
    GDrive,
    HDFS,
    HTTP,
    Local,
    S3,
    SSHMocked,
    OSS,
    TEST_CONFIG,
    TEST_GCP_CREDS_FILE,
    TEST_REMOTE,
)


class TestDataCloud(TestDvc):
    def _test_cloud(self, config, cl):
        self.dvc.config = config
        cloud = DataCloud(self.dvc)
        self.assertIsInstance(cloud.get_remote(), cl)

    def test(self):
        config = copy.deepcopy(TEST_CONFIG)

        clist = [
            ("s3://mybucket/", S3Remote),
            ("gs://mybucket/", GSRemote),
            ("ssh://user@localhost:/", SSHRemote),
            ("http://localhost:8000/", HTTPRemote),
            ("azure://ContainerName=mybucket;conn_string;", AzureRemote),
            ("oss://mybucket/", OSSRemote),
            (TestDvc.mkdtemp(), LocalRemote),
        ]

        for scheme, cl in clist:
            remote_url = scheme + str(uuid.uuid4())
            config["remote"][TEST_REMOTE]["url"] = remote_url
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
        config["remote"][TEST_REMOTE] = {"url": repo, "keyfile": keyfile}
        self.dvc.config = config
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
            remove(self.dvc.cache.local.cache_dir)

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


class TestS3Remote(S3, TestDataCloudBase):
    def _get_cloud_class(self):
        return S3Remote


def setup_gdrive_cloud(remote_url, dvc):
    config = copy.deepcopy(TEST_CONFIG)
    config["remote"][TEST_REMOTE] = {
        "url": remote_url,
        "gdrive_service_account_email": "test",
        "gdrive_service_account_p12_file_path": "test.p12",
        "gdrive_use_service_account": True,
    }

    dvc.config = config
    remote = DataCloud(dvc).get_remote()
    remote._gdrive_create_dir("root", remote.path_info.path)


class TestGDriveRemote(GDrive, TestDataCloudBase):
    def _setup_cloud(self):
        self._ensure_should_run()

        setup_gdrive_cloud(self.get_url(), self.dvc)

        self.cloud = DataCloud(self.dvc)
        remote = self.cloud.get_remote()
        self.assertIsInstance(remote, self._get_cloud_class())

    def _get_cloud_class(self):
        return GDriveRemote


class TestGSRemote(GCP, TestDataCloudBase):
    def _setup_cloud(self):
        self._ensure_should_run()

        repo = self.get_url()

        config = copy.deepcopy(TEST_CONFIG)
        config["remote"][TEST_REMOTE] = {
            "url": repo,
            "credentialpath": TEST_GCP_CREDS_FILE,
        }
        self.dvc.config = config
        self.cloud = DataCloud(self.dvc)

        self.assertIsInstance(self.cloud.get_remote(), self._get_cloud_class())

    def _get_cloud_class(self):
        return GSRemote


class TestAzureRemote(Azure, TestDataCloudBase):
    def _get_cloud_class(self):
        return AzureRemote


class TestOSSRemote(OSS, TestDataCloudBase):
    def _get_cloud_class(self):
        return OSSRemote


class TestLocalRemote(Local, TestDataCloudBase):
    def _get_cloud_class(self):
        return LocalRemote


@pytest.mark.usefixtures("ssh_server")
class TestSSHRemoteMocked(SSHMocked, TestDataCloudBase):
    @pytest.fixture(autouse=True)
    def setup_method_fixture(self, request, ssh_server):
        self.ssh_server = ssh_server
        self.method_name = request.function.__name__

    def _setup_cloud(self):
        self._ensure_should_run()

        repo = self.get_url()
        keyfile = self._get_keyfile()

        self._get_cloud_class().CAN_TRAVERSE = False
        config = copy.deepcopy(TEST_CONFIG)
        config["remote"][TEST_REMOTE] = {
            "url": repo,
            "keyfile": keyfile,
        }
        self.dvc.config = config
        self.cloud = DataCloud(self.dvc)

        self.assertIsInstance(self.cloud.get_remote(), self._get_cloud_class())

    def get_url(self):
        user = self.ssh_server.test_creds["username"]
        return super().get_url(user, self.ssh_server.port)

    def _get_keyfile(self):
        return self.ssh_server.test_creds["key_filename"]

    def _get_cloud_class(self):
        return SSHRemote


class TestHDFSRemote(HDFS, TestDataCloudBase):
    def _get_cloud_class(self):
        return HDFSRemote


@pytest.mark.usefixtures("http_server")
class TestHTTPRemote(HTTP, TestDataCloudBase):
    @pytest.fixture(autouse=True)
    def setup_method_fixture(self, request, http_server):
        self.http_server = http_server
        self.method_name = request.function.__name__

    def get_url(self):
        return super().get_url(self.http_server.server_port)

    def _get_cloud_class(self):
        return HTTPRemote


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

        remove(self.dvc.cache.local.cache_dir)

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
        self.main(["gc", "-cw", "-f"] + args)
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


class TestLocalRemoteCLI(Local, TestDataCloudCLIBase):
    def _test(self):
        url = self.get_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteHDFSCLI(HDFS, TestDataCloudCLIBase):
    def _test(self):
        url = self.get_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestS3RemoteCLI(S3, TestDataCloudCLIBase):
    def _test(self):
        url = self.get_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestGDriveRemoteCLI(GDrive, TestDataCloudCLIBase):
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
                "gdrive_service_account_email",
                "modified",
            ]
        )
        self.main(
            [
                "remote",
                "modify",
                TEST_REMOTE,
                "gdrive_service_account_p12_file_path",
                "modified.p12",
            ]
        )
        self.main(
            [
                "remote",
                "modify",
                TEST_REMOTE,
                "gdrive_use_service_account",
                "True",
            ]
        )

        self._test_cloud(TEST_REMOTE)


class TestGSRemoteCLI(GCP, TestDataCloudCLIBase):
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


class TestAzureRemoteCLI(Azure, TestDataCloudCLIBase):
    def _test(self):
        url = self.get_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestOSSRemoteCLI(OSS, TestDataCloudCLIBase):
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

        stage = self.dvc.run(
            outs=["bar"], cmd="echo bar > bar", single_stage=True
        )
        self.main(["push"])

        stage_file_path = stage.relpath
        content = load_stage_file(stage_file_path)
        del content["outs"][0]["md5"]
        dump_stage_file(stage_file_path, content)

        with self._caplog.at_level(logging.WARNING, logger="dvc"):
            self._caplog.clear()
            self.main(["status", "-c"])
            expected_warning = (
                "Output 'bar'(stage: 'bar.dvc') is missing version info. "
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
        return LocalRemote

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
        remove(self.dvc.cache.local.cache_dir)

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
    test_get_file_checksum = mocker.spy(LocalRemote, "get_file_checksum")
    url = Local.get_url()
    ret = main(["remote", "add", "-d", TEST_REMOTE, url])
    assert ret == 0
    ret = main(["config", "cache.type", "hardlink"])
    assert ret == 0
    ret = main(["add", "foo"])
    assert ret == 0
    ret = main(["push"])
    assert ret == 0
    ret = main(["run", "--single-stage", "-d", "foo", "echo foo"])
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
        remove(self.dvc.cache.local.cache_dir)

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

    dvc.config["remote"]["local_remote"] = {
        "url": fspath(tmp_path_factory.mktemp("local_remote"))
    }
    dvc.config["core"]["remote"] = "local_remote"
    dvc.push()

    # remove artifacts and cache to trigger fetching
    remove("file")
    remove("dir")
    remove(dvc.cache.local.cache_dir)

    checksum_spy = mocker.spy(dvc.cache.local, "get_file_checksum")

    dvc.pull()
    assert checksum_spy.call_count == 0

    # Removing cache will invalidate existing state entries
    remove(dvc.cache.local.cache_dir)

    dvc.config["remote"]["local_remote"]["verify"] = True

    dvc.pull()
    assert checksum_spy.call_count == 3


@pytest.mark.parametrize("erepo", ["git_dir", "erepo_dir"])
def test_pull_git_imports(request, tmp_dir, dvc, scm, erepo):
    erepo = request.getfixturevalue(erepo)
    with erepo.chdir():
        erepo.scm_gen({"dir": {"bar": "bar"}}, commit="second")
        erepo.scm_gen("foo", "foo", commit="first")

    dvc.imp(fspath(erepo), "foo")
    dvc.imp(fspath(erepo), "dir", out="new_dir", rev="HEAD~")

    assert dvc.pull()["downloaded"] == 0

    for item in ["foo", "new_dir", dvc.cache.local.cache_dir]:
        remove(item)
    os.makedirs(dvc.cache.local.cache_dir, exist_ok=True)
    clean_repos()

    assert dvc.pull(force=True)["downloaded"] == 2

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

    dvc.imp(fspath(erepo_dir), "foo")
    dvc.imp(fspath(erepo_dir), "dir", out="new_dir", rev="HEAD~")

    assert dvc.pull()["downloaded"] == 0

    clean(["foo", "new_dir"], dvc)

    assert dvc.pull(force=True)["downloaded"] == 2

    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / "foo").read_text() == "foo"

    assert (tmp_dir / "new_dir").exists()
    assert (tmp_dir / "new_dir" / "bar").read_text() == "bar"


def clean(outs, dvc=None):
    if dvc:
        outs = outs + [dvc.cache.local.cache_dir]
    for path in outs:
        print(path)
        remove(path)
    if dvc:
        os.makedirs(dvc.cache.local.cache_dir, exist_ok=True)
        clean_repos()


def recurse_list_dir(d):
    return [
        os.path.join(d, f) for _, _, filenames in os.walk(d) for f in filenames
    ]


def test_dvc_pull_pipeline_stages(tmp_dir, dvc, local_remote, run_copy):
    (stage0,) = tmp_dir.dvc_gen("foo", "foo")
    stage1 = run_copy("foo", "bar", single_stage=True)
    stage2 = run_copy("bar", "foobar", name="copy-bar-foobar")
    outs = ["foo", "bar", "foobar"]

    dvc.push()
    clean(outs, dvc)
    dvc.pull()
    assert all((tmp_dir / file).exists() for file in outs)

    for out, stage in zip(outs, [stage0, stage1, stage2]):
        for target in [stage.addressing, out]:
            clean(outs, dvc)
            stats = dvc.pull([target])
            assert stats["downloaded"] == 1
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


def test_pipeline_file_target_ops(tmp_dir, dvc, local_remote, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", single_stage=True)

    tmp_dir.dvc_gen("lorem", "lorem")
    run_copy("lorem", "lorem2", name="copy-lorem-lorem2")

    tmp_dir.dvc_gen("ipsum", "ipsum")
    run_copy("ipsum", "baz", name="copy-ipsum-baz")

    outs = ["foo", "bar", "lorem", "ipsum", "baz", "lorem2"]

    dvc.push()
    # each one's a copy of other, hence 3
    assert len(recurse_list_dir(fspath_py35(local_remote))) == 3

    clean(outs, dvc)
    assert set(dvc.pull(["dvc.yaml"])["added"]) == {"lorem2", "baz"}

    clean(outs, dvc)
    assert set(dvc.pull()["added"]) == set(outs)

    # clean everything in remote and push
    clean(local_remote.iterdir())
    dvc.push(["dvc.yaml:copy-ipsum-baz"])
    assert len(recurse_list_dir(fspath_py35(local_remote))) == 1

    clean(local_remote.iterdir())
    dvc.push(["dvc.yaml"])
    assert len(recurse_list_dir(fspath_py35(local_remote))) == 2

    with pytest.raises(StageNotFound):
        dvc.push(["dvc.yaml:StageThatDoesNotExist"])

    with pytest.raises(StageNotFound):
        dvc.pull(["dvc.yaml:StageThatDoesNotExist"])

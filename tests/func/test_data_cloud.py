from subprocess import CalledProcessError
from subprocess import check_output, Popen
from unittest import SkipTest
import os
import uuid
import shutil
import getpass
import platform
import copy
import logging
import pytest

from mock import patch

from dvc.utils.compat import str
from dvc.main import main
from dvc.config import Config
from dvc.data_cloud import (
    DataCloud,
    RemoteS3,
    RemoteGS,
    RemoteAzure,
    RemoteOSS,
    RemoteLOCAL,
    RemoteSSH,
    RemoteHDFS,
    RemoteHTTP,
)
from dvc.remote.base import STATUS_OK, STATUS_NEW, STATUS_DELETED
from dvc.utils import file_md5
from dvc.utils.stage import load_stage_file, dump_stage_file

from tests.basic_env import TestDvc
from tests.conftest import user
from tests.conftest import key_path
from tests.utils import spy


TEST_REMOTE = "upstream"
TEST_SECTION = 'remote "{}"'.format(TEST_REMOTE)
TEST_CONFIG = {
    Config.SECTION_CORE: {Config.SECTION_CORE_REMOTE: TEST_REMOTE},
    TEST_SECTION: {Config.SECTION_REMOTE_URL: ""},
}

TEST_AWS_REPO_BUCKET = "dvc-test"
TEST_GCP_REPO_BUCKET = "dvc-test"
TEST_OSS_REPO_BUCKET = "dvc-test"


def _should_test_aws():
    dvc_test_aws = os.getenv("DVC_TEST_AWS")
    if dvc_test_aws == "true":
        return True
    elif dvc_test_aws == "false":
        return False

    if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        return True

    return False


def _should_test_gcp():
    if os.getenv("DVC_TEST_GCP") == "true":
        return True

    if not os.path.exists(TestDvc.GCP_CREDS_FILE):
        return False

    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds and os.getenv("GCP_CREDS"):
        if os.path.exists(creds):
            os.unlink(creds)
        shutil.copyfile(TestDvc.GCP_CREDS_FILE, creds)
        try:
            check_output(
                [
                    "gcloud",
                    "auth",
                    "activate-service-account",
                    "--key-file",
                    creds,
                ]
            )
        except (CalledProcessError, OSError):
            return False
        return True

    return False


def _should_test_azure():
    if os.getenv("DVC_TEST_AZURE") == "true":
        return True
    elif os.getenv("DVC_TEST_AZURE") == "false":
        return False

    return os.getenv("AZURE_STORAGE_CONTAINER_NAME") and os.getenv(
        "AZURE_STORAGE_CONNECTION_STRING"
    )


def _should_test_oss():
    if os.getenv("DVC_TEST_OSS") == "true":
        return True
    elif os.getenv("DVC_TEST_OSS") == "false":
        return False

    return (
        os.getenv("OSS_ENDPOINT")
        and os.getenv("OSS_ACCESS_KEY_ID")
        and os.getenv("OSS_ACCESS_KEY_SECRET")
    )


def _should_test_ssh():
    if os.getenv("DVC_TEST_SSH") == "true":
        return True

    # FIXME: enable on windows
    if os.name == "nt":
        return False

    try:
        check_output(["ssh", "-o", "BatchMode=yes", "127.0.0.1", "ls"])
    except (CalledProcessError, IOError):
        return False

    return True


def _should_test_hdfs():
    if platform.system() != "Linux":
        return False

    try:
        check_output(
            ["hadoop", "version"], shell=True, executable=os.getenv("SHELL")
        )
    except (CalledProcessError, IOError):
        return False

    p = Popen(
        "hadoop fs -ls hdfs://127.0.0.1/",
        shell=True,
        executable=os.getenv("SHELL"),
    )
    p.communicate()
    if p.returncode != 0:
        return False

    return True


def get_local_storagepath():
    return TestDvc.mkdtemp()


def get_local_url():
    return get_local_storagepath()


def get_ssh_url():
    return "ssh://{}@127.0.0.1:{}".format(
        getpass.getuser(), get_local_storagepath()
    )


def get_ssh_url_mocked(user, port):
    path = get_local_storagepath()
    if os.name == "nt":
        # NOTE: On Windows get_local_storagepath() will return an ntpath
        # that looks something like `C:\some\path`, which is not compatible
        # with SFTP paths [1], so we need to convert it to a proper posixpath.
        # To do that, we should construct a posixpath that would be relative
        # to the server's root. In our case our ssh server is running with
        # `c:/` as a root, and our URL format requires absolute paths, so the
        # resulting path would look like `/some/path`.
        #
        # [1]https://tools.ietf.org/html/draft-ietf-secsh-filexfer-13#section-6
        drive, path = os.path.splitdrive(path)
        assert drive == "c:"
        path = path.replace("\\", "/")
    url = "ssh://{}@127.0.0.1:{}{}".format(user, port, path)
    return url


def get_hdfs_url():
    return "hdfs://{}@127.0.0.1{}".format(
        getpass.getuser(), get_local_storagepath()
    )


def get_aws_storagepath():
    return TEST_AWS_REPO_BUCKET + "/" + str(uuid.uuid4())


def get_aws_url():
    return "s3://" + get_aws_storagepath()


def get_gcp_storagepath():
    return TEST_GCP_REPO_BUCKET + "/" + str(uuid.uuid4())


def get_gcp_url():
    return "gs://" + get_gcp_storagepath()


def get_azure_url_compat():
    container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    assert container_name is not None
    return "azure://ContainerName={};{}".format(
        container_name, connection_string
    )


def get_azure_url():
    container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
    assert container_name is not None
    return "azure://{}/{}".format(container_name, str(uuid.uuid4()))


def get_oss_storagepath():
    return "{}/{}".format(TEST_OSS_REPO_BUCKET, (uuid.uuid4()))


def get_oss_url():
    return "oss://{}".format(get_oss_storagepath())


class TestDataCloud(TestDvc):
    def _test_cloud(self, config, cl):
        cloud = DataCloud(self.dvc, config=config)
        self.assertIsInstance(cloud._cloud, cl)

    def test(self):
        config = copy.deepcopy(TEST_CONFIG)

        clist = [
            ("s3://mybucket/", RemoteS3),
            ("gs://mybucket/", RemoteGS),
            ("ssh://user@localhost:/", RemoteSSH),
            ("http://localhost:8000/", RemoteHTTP),
            ("azure://ContainerName=mybucket;conn_string;", RemoteAzure),
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

    def _should_test(self):
        return False

    def _get_url(self):
        return ""

    def _get_keyfile(self):
        return None

    def _ensure_should_run(self):
        if not self._should_test():
            raise SkipTest(
                "Test {} is disabled".format(self.__class__.__name__)
            )

    def _setup_cloud(self):
        self._ensure_should_run()

        repo = self._get_url()
        keyfile = self._get_keyfile()

        config = copy.deepcopy(TEST_CONFIG)
        config[TEST_SECTION][Config.SECTION_REMOTE_URL] = repo
        config[TEST_SECTION][Config.SECTION_REMOTE_KEY_FILE] = keyfile
        self.cloud = DataCloud(self.dvc, config)

        self.assertIsInstance(self.cloud._cloud, self._get_cloud_class())

    def _test_cloud(self):
        self._setup_cloud()

        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)
        cache = stage.outs[0].cache_path
        info = stage.outs[0].dumpd()
        md5 = info["md5"]

        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        stage_dir = stages[0]
        self.assertTrue(stage_dir is not None)

        cache_dir = stage_dir.outs[0].cache_path
        info_dir = stage_dir.outs[0].dumpd()
        md5_dir = info_dir["md5"]

        with self.cloud.repo.state:
            # Check status
            status = self.cloud.status([info], show_checksums=True)
            expected = {md5: {"name": md5, "status": STATUS_NEW}}
            self.assertEqual(status, expected)

            status_dir = self.cloud.status([info_dir], show_checksums=True)
            expected = {md5_dir: {"name": md5_dir, "status": STATUS_NEW}}
            self.assertEqual(status_dir, expected)

            # Push and check status
            self.cloud.push([info])
            self.assertTrue(os.path.exists(cache))
            self.assertTrue(os.path.isfile(cache))

            self.cloud.push([info_dir])
            self.assertTrue(os.path.isfile(cache_dir))

            status = self.cloud.status([info], show_checksums=True)
            expected = {md5: {"name": md5, "status": STATUS_OK}}
            self.assertEqual(status, expected)

            status_dir = self.cloud.status([info_dir], show_checksums=True)
            expected = {md5_dir: {"name": md5_dir, "status": STATUS_OK}}
            self.assertEqual(status_dir, expected)

            # Remove and check status
            shutil.rmtree(self.dvc.cache.local.cache_dir)

            status = self.cloud.status([info], show_checksums=True)
            expected = {md5: {"name": md5, "status": STATUS_DELETED}}
            self.assertEqual(status, expected)

            status_dir = self.cloud.status([info_dir], show_checksums=True)
            expected = {md5_dir: {"name": md5_dir, "status": STATUS_DELETED}}
            self.assertEqual(status_dir, expected)

            # Pull and check status
            self.cloud.pull([info])
            self.assertTrue(os.path.exists(cache))
            self.assertTrue(os.path.isfile(cache))
            with open(cache, "r") as fd:
                self.assertEqual(fd.read(), self.FOO_CONTENTS)

            self.cloud.pull([info_dir])
            self.assertTrue(os.path.isfile(cache_dir))

            status = self.cloud.status([info], show_checksums=True)
            expected = {md5: {"name": md5, "status": STATUS_OK}}
            self.assertEqual(status, expected)

            status_dir = self.cloud.status([info_dir], show_checksums=True)
            expected = {md5_dir: {"name": md5_dir, "status": STATUS_OK}}
            self.assertTrue(status_dir, expected)

    def test(self):
        self._ensure_should_run()
        self._test_cloud()


class TestRemoteS3(TestDataCloudBase):
    def _should_test(self):
        return _should_test_aws()

    def _get_url(self):
        return get_aws_url()

    def _get_cloud_class(self):
        return RemoteS3


class TestRemoteGS(TestDataCloudBase):
    def _should_test(self):
        return _should_test_gcp()

    def _setup_cloud(self):
        self._ensure_should_run()

        repo = self._get_url()

        config = copy.deepcopy(TEST_CONFIG)
        config[TEST_SECTION][Config.SECTION_REMOTE_URL] = repo
        config[TEST_SECTION][
            Config.SECTION_GCP_CREDENTIALPATH
        ] = TestDvc.GCP_CREDS_FILE
        self.cloud = DataCloud(self.dvc, config)

        self.assertIsInstance(self.cloud._cloud, self._get_cloud_class())

    def _get_url(self):
        return get_gcp_url()

    def _get_cloud_class(self):
        return RemoteGS


class TestRemoteAzureCompat(TestDataCloudBase):
    def _should_test(self):
        return _should_test_azure()

    def _get_url(self):
        return get_azure_url_compat()

    def _get_cloud_class(self):
        return RemoteAzure


class TestRemoteAzure(TestRemoteAzureCompat):
    def _get_url(self):
        return get_azure_url()


class TestRemoteOSS(TestDataCloudBase):
    def _should_test(self):
        return _should_test_oss()

    def _get_url(self):
        return get_oss_url()

    def _get_cloud_class(self):
        return RemoteOSS


class TestRemoteLOCAL(TestDataCloudBase):
    def _should_test(self):
        return True

    def _get_url(self):
        self.dname = get_local_url()
        return self.dname

    def _get_cloud_class(self):
        return RemoteLOCAL

    def test(self):
        super(TestRemoteLOCAL, self).test()
        self.assertTrue(os.path.isdir(self.dname))


class TestRemoteSSH(TestDataCloudBase):
    def _should_test(self):
        return _should_test_ssh()

    def _get_url(self):
        return get_ssh_url()

    def _get_cloud_class(self):
        return RemoteSSH


@pytest.mark.usefixtures("ssh_server")
class TestRemoteSSHMocked(TestDataCloudBase):
    @pytest.fixture(autouse=True)
    def setup_method_fixture(self, request, ssh_server):
        self.ssh_server = ssh_server
        self.method_name = request.function.__name__

    def _get_url(self):
        return get_ssh_url_mocked(user, self.ssh_server.port)

    def _get_keyfile(self):
        return key_path

    def _should_test(self):
        return True

    def _get_cloud_class(self):
        return RemoteSSH


class TestRemoteHDFS(TestDataCloudBase):
    def _should_test(self):
        return _should_test_hdfs()

    def _get_url(self):
        return get_hdfs_url()

    def _get_cloud_class(self):
        return RemoteHDFS


class TestDataCloudCLIBase(TestDvc):
    def main(self, args):
        ret = main(args)
        self.assertEqual(ret, 0)

    def _test_cloud(self, remote=None):
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
        self.main(["status", "-c", "--show-checksums"] + args)

        self.main(["push"] + args)
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        self.assertTrue(os.path.isfile(cache_dir))

        self.main(["status", "-c", "--show-checksums"] + args)

        shutil.rmtree(self.dvc.cache.local.cache_dir)

        self.main(["status", "-c", "--show-checksums"] + args)

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

        self.main(["status", "-c", "--show-checksums"] + args)

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

    def _should_test(self):
        return True

    def _test(self):
        pass

    def test(self):
        if not self._should_test():
            raise SkipTest(
                "Test {} is disabled".format(self.__class__.__name__)
            )
        self._test()


class TestCompatRemoteLOCALCLI(TestDataCloudCLIBase):
    def _test(self):
        storagepath = get_local_storagepath()
        self.main(["config", "core.cloud", "local"])
        self.main(["config", "local.storagepath", storagepath])

        self._test_cloud()


class TestRemoteLOCALCLI(TestDataCloudCLIBase):
    def _test(self):
        url = get_local_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteSSHCLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_ssh()

    def _test(self):
        url = get_ssh_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteHDFSCLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_hdfs()

    def _test(self):
        url = get_hdfs_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestCompatRemoteS3CLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_aws()

    def _test(self):
        storagepath = get_aws_storagepath()
        self.main(["config", "core.cloud", "aws"])
        self.main(["config", "aws.storagepath", storagepath])

        self._test_cloud()


class TestRemoteS3CLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_aws()

    def _test(self):
        url = get_aws_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestCompatRemoteGSCLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_gcp()

    def _test(self):
        storagepath = get_gcp_storagepath()
        self.main(["config", "core.cloud", "gcp"])
        self.main(["config", "gcp.storagepath", storagepath])

        self._test_cloud()


class TestRemoteGSCLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_gcp()

    def _test(self):
        url = get_gcp_url()

        self.main(["remote", "add", TEST_REMOTE, url])
        self.main(
            [
                "remote",
                "modify",
                TEST_REMOTE,
                "credentialpath",
                TestDvc.GCP_CREDS_FILE,
            ]
        )

        self._test_cloud(TEST_REMOTE)


class TestRemoteAzureCLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_azure()

    def _test(self):
        url = get_azure_url()

        self.main(["remote", "add", TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteOSSCLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_oss()

    def _test(self):
        url = get_oss_url()

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
        url = get_local_url()
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
                "Output 'bar'(Stage: 'bar.dvc') is missing version info."
                " Cache for it will not be collected."
                " Use dvc repro to get your pipeline up to date."
            )

            assert expected_warning in self._caplog.text

    def test(self):
        self._test()


class TestRecursiveSyncOperations(TestDataCloudBase):
    def main(self, args):
        ret = main(args)
        self.assertEqual(ret, 0)

    def _get_url(self):
        self.dname = get_local_url()
        return self.dname

    def _should_test(self):
        return True

    def _get_cloud_class(self):
        return RemoteLOCAL

    def _prepare_repo(self):
        self.main(
            ["remote", "add", "-d", TEST_REMOTE, self.cloud._cloud.cache_dir]
        )

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
        cloud_data_path = self.cloud._cloud.get(data_md5)
        cloud_data_sub_path = self.cloud._cloud.get(data_sub_md5)

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


class TestCheckSumRecalculation(TestDvc):
    def test(self):
        test_get_file_checksum = spy(RemoteLOCAL.get_file_checksum)
        with patch.object(
            RemoteLOCAL, "get_file_checksum", test_get_file_checksum
        ):
            url = get_local_url()
            ret = main(["remote", "add", "-d", TEST_REMOTE, url])
            self.assertEqual(ret, 0)
            ret = main(["config", "cache.type", "hardlink"])
            self.assertEqual(ret, 0)
            ret = main(["add", self.FOO])
            self.assertEqual(ret, 0)
            ret = main(["push"])
            self.assertEqual(ret, 0)
            ret = main(["run", "-d", self.FOO, "echo foo"])
            self.assertEqual(ret, 0)
        self.assertEqual(test_get_file_checksum.mock.call_count, 1)


class TestShouldWarnOnNoChecksumInLocalAndRemoteCache(TestDvc):
    def setUp(self):
        super(TestShouldWarnOnNoChecksumInLocalAndRemoteCache, self).setUp()

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
            "nor on remote. Missing cache files: \n"
        )
        self.message_bar_part = "\nname: {}, md5: {}\n".format(
            self.BAR, checksum_bar
        )
        self.message_foo_part = "\nname: {}, md5: {}\n".format(
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

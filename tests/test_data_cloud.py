from subprocess import CalledProcessError
from subprocess import check_output
from unittest import SkipTest
import os
import time
import uuid
import shutil
import getpass
import tempfile
import platform

from dvc.main import main
from dvc.config import Config, ConfigError
from dvc.data_cloud import (DataCloud, RemoteS3, RemoteGS, RemoteAzure,
                            RemoteLOCAL, RemoteSSH, RemoteHDFS)
from dvc.remote.base import STATUS_OK, STATUS_NEW, STATUS_DELETED

from tests.basic_env import TestDvc


TEST_REMOTE = 'upstream'
TEST_SECTION = 'remote "{}"'.format(TEST_REMOTE)
TEST_CONFIG = {Config.SECTION_CORE: {Config.SECTION_CORE_REMOTE: TEST_REMOTE},
               TEST_SECTION: {Config.SECTION_REMOTE_URL: ''}}

TEST_AWS_REPO_BUCKET = 'dvc-test'
TEST_GCP_REPO_BUCKET = 'dvc-test'


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

    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds and os.getenv("GCP_CREDS"):
        if os.path.exists(creds):
            os.unlink(creds)
        shutil.copyfile(TestDvc.GCP_CREDS_FILE, creds)
        try:
            check_output(['gcloud', 'auth', 'activate-service-account',
                          '--key-file', creds])
        except CalledProcessError:
            return False
        return True

    return False


def _should_test_azure():
    if os.getenv("DVC_TEST_AZURE") == "true":
        return True
    elif os.getenv("DVC_TEST_AZURE") == "false":
        return False

    return (os.getenv("AZURE_STORAGE_CONTAINER_NAME")
            and os.getenv("AZURE_STORAGE_CONNECTION_STRING"))


def _should_test_ssh():
    if os.getenv("DVC_TEST_SSH") == "true":
        return True

    # FIXME: enable on windows
    if os.name == 'nt':
        return False

    try:
        check_output(['ssh', '127.0.0.1', 'ls'])
    except (CalledProcessError, FileNotFoundError):
        return False

    return True


def _should_test_hdfs():
    if platform.system() != 'Linux':
        return False

    try:
        check_output(['hadoop', 'version'])
    except (CalledProcessError, FileNotFoundError):
        return False

    return True


def get_local_storagepath():
    return tempfile.mkdtemp()


def get_local_url():
    return get_local_storagepath()


def get_ssh_url():
    return 'ssh://{}@127.0.0.1:{}'.format(
        getpass.getuser(),
        get_local_storagepath())


def get_hdfs_url():
    return 'hdfs://{}@127.0.0.1{}'.format(
        getpass.getuser(),
        get_local_storagepath())


def get_aws_storagepath():
    return TEST_AWS_REPO_BUCKET + '/' + str(uuid.uuid4())


def get_aws_url():
    return 's3://' + get_aws_storagepath()


def get_gcp_storagepath():
    return TEST_GCP_REPO_BUCKET + '/' + str(uuid.uuid4())


def get_gcp_url():
    return 'gs://' + get_gcp_storagepath()


def get_azure_url():
    return 'azure://'


def sleep():
    # Sleep some time to simulate realistic behavior.
    # Some filesystems have a bad date resolution for
    # mtime(i.e. 1sec for HFS) that cause problems with
    # our 'state' system not being able to distinguish
    # files that were modified within that delta.
    time.sleep(1)


class TestDataCloud(TestDvc):
    def _test_cloud(self, config, cl):
        cloud = DataCloud(self.dvc, config=config)
        self.assertIsInstance(cloud._cloud, cl)

    def test(self):
        config = TEST_CONFIG

        clist = [('s3://', RemoteS3),
                 ('gs://', RemoteGS),
                 ('ssh://user@localhost:/', RemoteSSH),
                 (tempfile.mkdtemp(), RemoteLOCAL)]

        if _should_test_azure():
            clist.append(('azure://ContainerName=', RemoteAzure))

        for scheme, cl in clist:
            remote_url = scheme + str(uuid.uuid4())
            config[TEST_SECTION][Config.SECTION_REMOTE_URL] = remote_url
            self._test_cloud(config, cl)

    def test_unsupported(self):
        with self.assertRaises(ConfigError):
            remote_url = 'notsupportedscheme://a/b'
            config = TEST_CONFIG
            config[TEST_SECTION][Config.SECTION_REMOTE_URL] = remote_url
            DataCloud(self.dvc, config=config)


class TestDataCloudBase(TestDvc):
    def _get_cloud_class(self):
        return None

    def _should_test(self):
        return False

    def _get_url(self):
        return ''

    def _ensure_should_run(self):
        if not self._should_test():
            raise SkipTest('Test {} is disabled'
                           .format(self.__class__.__name__))

    def _setup_cloud(self):
        self._ensure_should_run()

        repo = self._get_url()

        config = TEST_CONFIG
        config[TEST_SECTION][Config.SECTION_REMOTE_URL] = repo
        self.cloud = DataCloud(self.dvc, config)

        self.assertIsInstance(self.cloud._cloud, self._get_cloud_class())

    def _test_cloud(self):
        self._setup_cloud()

        stage = self.dvc.add(self.FOO)
        cache = stage.outs[0].cache
        info = stage.outs[0].info
        md5 = info['md5']

        stage_dir = self.dvc.add(self.DATA_DIR)
        cache_dir = stage_dir.outs[0].cache
        info_dir = stage_dir.outs[0].info
        md5_dir = info_dir['md5']

        # Check status
        sleep()
        status = self.cloud.status([info])
        self.assertEqual([(md5, STATUS_NEW)], status)

        status_dir = self.cloud.status([info_dir])
        self.assertTrue((md5_dir, STATUS_NEW) in status_dir)

        # Push and check status
        self.cloud.push([info])
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))

        self.cloud.push([info_dir])
        self.assertTrue(os.path.isfile(cache_dir))

        sleep()
        status = self.cloud.status([info])
        self.assertEqual([(md5, STATUS_OK)], status)

        status_dir = self.cloud.status([info_dir])
        self.assertTrue((md5_dir, STATUS_OK) in status_dir)

        # Remove and check status
        sleep()
        shutil.rmtree(self.dvc.cache.local.cache_dir)

        status = self.cloud.status([info])
        self.assertEqual([(md5, STATUS_DELETED)], status)

        status_dir = self.cloud.status([info_dir])
        self.assertEqual([(md5_dir, STATUS_DELETED)], status_dir)

        # Pull and check status
        self.cloud.pull([info])
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        with open(cache, 'r') as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)

        self.cloud.pull([info_dir])
        self.assertTrue(os.path.isfile(cache_dir))

        sleep()
        status = self.cloud.status([info])
        self.assertEqual([(md5, STATUS_OK)], status)

        status_dir = self.cloud.status([info_dir])
        self.assertTrue((md5_dir, STATUS_OK) in status_dir)

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

    def _get_url(self):
        return get_gcp_url()

    def _get_cloud_class(self):
        return RemoteGS


class TestRemoteAzure(TestDataCloudBase):
    def _should_test(self):
        return _should_test_azure()

    def _get_url(self):
        return get_azure_url()

    def _get_cloud_class(self):
        return RemoteAzure


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

    def test(self):
        super(TestRemoteSSH, self).test()


class TestRemoteHDFS(TestDataCloudBase):
    def _should_test(self):
        return _should_test_hdfs()

    def _get_url(self):
        return get_hdfs_url()

    def _get_cloud_class(self):
        return RemoteHDFS

    def test(self):
        super(TestRemoteHDFS, self).test()


class TestDataCloudCLIBase(TestDvc):
    def main(self, args):
        ret = main(args)
        self.assertEqual(ret, 0)

    def _test_cloud(self, remote=None):
        args = ['-v', '-j', '2']
        if remote:
            args += ['-r', remote]
        else:
            args += []

        stage = self.dvc.add(self.FOO)
        cache = stage.outs[0].cache

        stage_dir = self.dvc.add(self.DATA_DIR)
        cache_dir = stage_dir.outs[0].cache

        # FIXME check status output
        sleep()
        self.main(['status', '-c'] + args)

        self.main(['push'] + args)
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        self.assertTrue(os.path.isfile(cache_dir))

        sleep()
        self.main(['status', '-c'] + args)

        shutil.rmtree(self.dvc.cache.local.cache_dir)

        sleep()
        self.main(['status', '-c'] + args)

        self.main(['pull'] + args)
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))

        sleep()
        with open(cache, 'r') as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)
        self.assertTrue(os.path.isfile(cache_dir))

        self.main(['status', '-c'] + args)

    def _should_test(self):
        return True

    def _test(self):
        pass

    def test(self):
        if self._should_test():
            self._test()


class TestCompatRemoteLOCALCLI(TestDataCloudCLIBase):
    def _test(self):
        storagepath = get_local_storagepath()
        self.main(['config', 'core.cloud', 'local'])
        self.main(['config', 'local.storagepath', storagepath])

        self._test_cloud()


class TestRemoteLOCALCLI(TestDataCloudCLIBase):
    def _test(self):
        url = get_local_url()

        self.main(['remote', 'add', TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteSSHCLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_ssh()

    def _test(self):
        url = get_ssh_url()

        self.main(['remote', 'add', TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteHDFSCLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_hdfs()

    def _test(self):
        url = get_hdfs_url()

        self.main(['remote', 'add', TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestCompatRemoteS3CLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_aws()

    def _test(self):
        storagepath = get_aws_storagepath()
        self.main(['config', 'core.cloud', 'aws'])
        self.main(['config', 'aws.storagepath', storagepath])

        self._test_cloud()


class TestRemoteS3CLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_aws()

    def _test(self):
        url = get_aws_url()

        self.main(['remote', 'add', TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestCompatRemoteGSCLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_gcp()

    def _test(self):
        storagepath = get_gcp_storagepath()
        self.main(['config', 'core.cloud', 'gcp'])
        self.main(['config', 'gcp.storagepath', storagepath])

        self._test_cloud()


class TestRemoteGSCLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_gcp()

    def _test(self):
        url = get_gcp_url()

        self.main(['remote', 'add', TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteAzureCLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_azure()

    def _test(self):
        url = get_azure_url()

        self.main(['remote', 'add', TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestDataCloudErrorCLI(TestDvc):
    def main_fail(self, args):
        ret = main(args)
        self.assertNotEqual(ret, 0)

    def test_error(self):
        f = 'non-existing-file'
        self.main_fail(['status', '-c', f])
        self.main_fail(['push', f])
        self.main_fail(['pull', f])
        self.main_fail(['fetch', f])

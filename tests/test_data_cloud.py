import os
import time
import stat
import uuid
import shutil
import getpass
import tempfile

from dvc.main import main
from dvc.config import Config, ConfigError
from dvc.data_cloud import DataCloud, RemoteS3, RemoteGS, RemoteLOCAL
from dvc.remote.base import STATUS_UNKNOWN, STATUS_OK, STATUS_MODIFIED, STATUS_NEW, STATUS_DELETED

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
        return True

    return False


def _should_test_ssh():
    if os.getenv("DVC_TEST_SSH") == "true":
        return True

    #FIXME: enable on windows
    if os.name == 'nt':
        return False

    assert os.system('ssh 127.0.0.1 ls &> /dev/null') == 0

    return True


def get_local_storagepath():
    return tempfile.mkdtemp()


def get_local_url():
    return get_local_storagepath()


def get_ssh_url():
    return '{}@127.0.0.1:{}'.format(getpass.getuser(), get_local_storagepath())


def get_aws_storagepath():
    return TEST_AWS_REPO_BUCKET + '/' + str(uuid.uuid4())


def get_aws_url():
    return 's3://' + get_aws_storagepath()


def get_gcp_storagepath():
    return TEST_GCP_REPO_BUCKET + '/' + str(uuid.uuid4())


def get_gcp_url():
    return 'gs://' + get_gcp_storagepath()


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

        for scheme, cl in [('s3://', RemoteS3), ('gs://', RemoteGS), (tempfile.mkdtemp(), RemoteLOCAL)]:
            config[TEST_SECTION][Config.SECTION_REMOTE_URL] = scheme + str(uuid.uuid4())
            self._test_cloud(config, cl)

    def test_unsupported(self):
        with self.assertRaises(ConfigError) as cx:
            config = TEST_CONFIG
            config[TEST_SECTION][Config.SECTION_REMOTE_URL] = 'notsupportedscheme://a/b'
            DataCloud(self.dvc, config=config)


class TestDataCloudBase(TestDvc):
    def _should_test(self):
        return False

    def _get_url(self):
        return None

    @property
    def cloud_class(self):
        return None

    def _setup_cloud(self):
        if not self._should_test():
            return

        repo = self._get_url()

        config = TEST_CONFIG
        config[TEST_SECTION][Config.SECTION_REMOTE_URL] = repo
        self.cloud = self._get_cloud_class()(self.dvc, config[TEST_SECTION])

    def _test_cloud(self):
        self._setup_cloud()

        stage = self.dvc.add(self.FOO)
        cache = stage.outs[0].cache

        stage_dir = self.dvc.add(self.DATA_DIR)
        cache_dir = stage_dir.outs[0].cache

        # Check status
        sleep()
        status = self.cloud.status(cache)
        self.assertEqual(status, STATUS_NEW)

        status_dir = self.cloud.status(cache_dir)
        self.assertEqual(status_dir, STATUS_NEW)

        # Push and check status
        self.cloud.push(cache)
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))

        self.cloud.push(cache_dir)
        self.assertTrue(os.path.isfile(cache_dir))

        sleep()
        status = self.cloud.status(cache)
        self.assertEqual(status, STATUS_OK)

        status_dir = self.cloud.status(cache_dir)
        self.assertEqual(status_dir, STATUS_OK)

        # Modify and check status
        sleep()
        with open(cache, 'a') as fd:
            fd.write('addon')
        status = self.cloud.status(cache)
        # NOTE: this is what corrupted cache looks like and it should
        # be removed automatically, thus causing STATUS_DELETED instead
        # of STATUS_MODIFIED.
        self.assertEqual(status, STATUS_DELETED)

        # Remove and check status
        sleep()
        shutil.rmtree(self.dvc.cache.local.cache_dir)

        status = self.cloud.status(cache)
        self.assertEqual(status, STATUS_DELETED)

        status_dir = self.cloud.status(cache_dir)
        self.assertEqual(status_dir, STATUS_DELETED)

        # Pull and check status
        self.cloud.pull(cache)
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        with open(cache, 'r') as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)

        self.cloud.pull(cache_dir)
        self.assertTrue(os.path.isfile(cache_dir))

        sleep()
        status = self.cloud.status(cache)
        self.assertEqual(status, STATUS_OK)

        status_dir = self.cloud.status(cache_dir)
        self.assertEqual(status_dir, STATUS_OK)

    def test(self):
        if self._should_test():
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


class TestDataCloudCLIBase(TestDvc):
    def main(self, args):
        ret = main(args)
        self.assertEqual(ret, 0)

    def _test_cloud(self, remote=None):
        args = ['-j', '2']
        if remote:
            args += ['-r', remote]
        else:
            args += []

        stage = self.dvc.add(self.FOO)
        cache = stage.outs[0].cache

        stage_dir = self.dvc.add(self.DATA_DIR)
        cache_dir = stage_dir.outs[0].cache

        #FIXME check status output
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

    def _test_compat(self):
        pass

    def test(self):
        if self._should_test():
            self._test()
            self._test_compat()


class TestRemoteLOCALCLI(TestDataCloudCLIBase):
    def _test_compat(self):
        storagepath = get_local_storagepath()
        self.main(['config', 'core.cloud', 'local'])
        self.main(['config', 'local.storagepath', storagepath])

        self._test_cloud()

    def _test(self):
        url = get_local_url()

        self.main(['remote', 'add', TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteSSHCLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_ssh()

    def _test_compat(self):
        # We do not support ssh in legacy mode
        pass

    def _test(self):
        url = get_ssh_url()

        self.main(['remote', 'add', TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteS3CLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_aws()

    def _test_compat(self):
        storagepath = get_aws_storagepath()
        self.main(['config', 'core.cloud', 'aws'])
        self.main(['config', 'aws.storagepath', storagepath])

        self._test_cloud()

    def _test(self):
        url = get_aws_url()

        self.main(['remote', 'add', TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestRemoteGSCLI(TestDataCloudCLIBase):
    def _should_test(self):
        return _should_test_gcp()

    def _test_compat(self):
        storagepath = get_gcp_storagepath()
        self.main(['config', 'core.cloud', 'gcp'])
        self.main(['config', 'gcp.storagepath', storagepath])

        self._test_cloud()

    def _test(self):
        url = get_gcp_url()

        self.main(['remote', 'add', TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestDataCloudErrorCLI(TestDvc):
    def main_fail(self, args):
        ret = main(args)
        self.assertNotEqual(ret, 0)

    def test_error(self):
        f = 'non-existing-file'
        self.main_fail(['status', f])
        self.main_fail(['push', f])
        self.main_fail(['pull', f])
        self.main_fail(['fetch', f])

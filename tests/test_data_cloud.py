import os
import stat
import uuid
import shutil
import tempfile

from dvc.main import main
from dvc.config import Config, ConfigError
from dvc.cloud.data_cloud import DataCloud, DataCloudAWS, DataCloudLOCAL, DataCloudGCP
from dvc.cloud.base import STATUS_UNKNOWN, STATUS_OK, STATUS_MODIFIED, STATUS_NEW, STATUS_DELETED
from dvc.cloud.instance_manager import CloudSettings

from tests.basic_env import TestDvc


TEST_REMOTE = 'upstream'
TEST_SECTION = 'remote "{}"'.format(TEST_REMOTE)
TEST_CONFIG = {Config.SECTION_CORE: {Config.SECTION_CORE_REMOTE: TEST_REMOTE},
               TEST_SECTION: {Config.SECTION_REMOTE_URL: ''}}

TEST_AWS_REPO_BUCKET = 'dvc-test'
TEST_AWS_REPO_REGION = 'us-east-2'

TEST_GCP_REPO_BUCKET = 'dvc-test'


def should_test_aws():
    dvc_test_aws = os.getenv("DVC_TEST_AWS")
    if dvc_test_aws == "true":
        return True
    elif dvc_test_aws == "false":
        return False

    if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        return True

    return False


def should_test_gcp():
    if os.getenv("DVC_TEST_GCP") == "true":
        return True

    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds and os.getenv("GCP_CREDS"):
        if os.path.exists(creds):
            os.unlink(creds)
        shutil.copyfile(TestDvc.GCP_CREDS_FILE, creds)
        return True

    return False


def get_local_storagepath():
    return tempfile.mkdtemp()


def get_local_url():
    return get_local_storagepath()


def get_aws_storagepath():
    return TEST_AWS_REPO_BUCKET + '/' + str(uuid.uuid4())


def get_aws_url():
    return 's3://' + get_aws_storagepath()


def get_gcp_storagepath():
    return TEST_GCP_REPO_BUCKET + '/' + str(uuid.uuid4())


def get_gcp_url():
    return 'gs://' + get_gcp_storagepath()


class TestDataCloud(TestDvc):
    def _test_cloud(self, config, cl):
        cloud = DataCloud(self.dvc.cache, config)
        self.assertIsInstance(cloud._cloud, cl)

    def test(self):
        config = TEST_CONFIG

        for scheme, cl in [('s3://', DataCloudAWS), ('gs://', DataCloudGCP), ('/', DataCloudLOCAL)]:
            config[TEST_SECTION][Config.SECTION_REMOTE_URL] = scheme + 'a/b'
            self._test_cloud(config, cl)

    def test_unsupported(self):
        with self.assertRaises(ConfigError) as cx:
            config = TEST_CONFIG
            config[TEST_SECTION][Config.SECTION_REMOTE_URL] = 'notsupportedscheme://a/b'
            DataCloud(self.dvc.cache, config)


class TestDataCloudBase(TestDvc):
    def _setup_cloud(self):
        self.cloud = None

    def _test_cloud(self):
        self._setup_cloud()

        self.cloud.connect()

        stage = self.dvc.add(self.FOO)
        cache = stage.outs[0].cache

        stage_dir = self.dvc.add(self.DATA_DIR)
        cache_dir = stage_dir.outs[0].cache

        # Check status
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

        status = self.cloud.status(cache)
        self.assertEqual(status, STATUS_OK)

        status_dir = self.cloud.status(cache_dir)
        self.assertEqual(status_dir, STATUS_OK)

        # Modify and check status
        with open(cache, 'a') as fd:
            fd.write('addon')
        status = self.cloud.status(cache)
        self.assertEqual(status, STATUS_MODIFIED)

        # Remove and check status
        shutil.rmtree(self.dvc.cache.cache_dir)

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

        status = self.cloud.status(cache)
        self.assertEqual(status, STATUS_OK)

        status_dir = self.cloud.status(cache_dir)
        self.assertEqual(status_dir, STATUS_OK)


class TestDataCloudAWS(TestDataCloudBase):
    def _setup_cloud(self):
        if not should_test_aws():
            return

        repo = get_aws_url()

        # Setup cloud
        config = TEST_CONFIG
        config[TEST_SECTION][Config.SECTION_REMOTE_URL] = repo
        config[TEST_SECTION][Config.SECTION_AWS_REGION] = TEST_AWS_REPO_REGION
        cloud_settings = CloudSettings(self.dvc.cache, None, config[TEST_SECTION])
        self.cloud = DataCloudAWS(cloud_settings)

    def test(self):
        if should_test_aws():
            self._test_cloud()


class TestDataCloudGCP(TestDataCloudBase):
    def _setup_cloud(self):
        if not should_test_gcp():
            return

        repo = get_gcp_url()

        # Setup cloud
        config = TEST_CONFIG
        config[TEST_SECTION][Config.SECTION_REMOTE_URL] = repo
        cloud_settings = CloudSettings(self.dvc.cache, None, config[TEST_SECTION])
        self.cloud = DataCloudGCP(cloud_settings)

    def test(self):
        if should_test_gcp():
            self._test_cloud()


class TestDataCloudLOCAL(TestDataCloudBase):
    def _setup_cloud(self):
        self.dname = get_local_url()

        config = TEST_CONFIG
        config[TEST_SECTION][Config.SECTION_REMOTE_URL] = self.dname
        cloud_settings = CloudSettings(self.dvc.cache, None, config[TEST_SECTION])
        self.cloud = DataCloudLOCAL(cloud_settings)

    def test(self):
        self._test_cloud()
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
        self.main(['status', '-c'] + args)

        self.main(['push'] + args)
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        self.assertTrue(os.path.isfile(cache_dir))

        self.main(['status', '-c'] + args)

        shutil.rmtree(self.dvc.cache.cache_dir)

        self.main(['status', '-c'] + args)

        self.main(['pull'] + args)
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        with open(cache, 'r') as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)
        self.assertTrue(os.path.isfile(cache_dir))

        self.main(['status', '-c'] + args)

    def should_test(self):
        return True

    def _test(self):
        pass

    def _test_compat(self):
        pass

    def test(self):
        if self.should_test():
            self._test()
            self._test_compat()


class TestDataCloudLOCALCLI(TestDataCloudCLIBase):
    def _test_compat(self):
        storagepath = get_local_storagepath()
        self.main(['config', 'core.cloud', 'local'])
        self.main(['config', 'local.storagepath', storagepath])

        self._test_cloud()

    def _test(self):
        url = get_local_url()

        self.main(['remote', 'add', TEST_REMOTE, url])

        self._test_cloud(TEST_REMOTE)


class TestDataCloudAWSCLI(TestDataCloudCLIBase):
    def should_test(self):
        return should_test_aws()

    def _test_compat(self):
        storagepath = get_aws_storagepath()
        self.main(['config', 'core.cloud', 'aws'])
        self.main(['config', 'aws.storagepath', storagepath])
        self.main(['config', 'aws.region', TEST_AWS_REPO_REGION])

        self._test_cloud()

    def _test(self):
        url = get_aws_url()

        self.main(['remote', 'add', TEST_REMOTE, url])
        self.main(['remote', 'modify', TEST_REMOTE, Config.SECTION_AWS_REGION, TEST_AWS_REPO_REGION])

        self._test_cloud(TEST_REMOTE)


class TestDataCloudGCPCLI(TestDataCloudCLIBase):
    def should_test(self):
        return should_test_gcp()

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

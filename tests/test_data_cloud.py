import os
import stat
import uuid
import shutil

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
    TEST_REPO_BUCKET = 'dvc-test'
    TEST_REPO_REGION = 'us-east-2'

    def _should_test(self):
        dvc_test_aws = os.getenv("DVC_TEST_AWS")
        if dvc_test_aws == "true":
            return True
        elif dvc_test_aws == "false":
            return False

        if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
            return True

        return False

    def _setup_cloud(self):
        if not self._should_test():
            return

        repo = 's3://' + self.TEST_REPO_BUCKET + '/' + str(uuid.uuid4())

        # Setup cloud
        config = TEST_CONFIG
        config[TEST_SECTION][Config.SECTION_REMOTE_URL] = repo
        config[TEST_SECTION][Config.SECTION_AWS_REGION] = self.TEST_REPO_REGION
        cloud_settings = CloudSettings(self.dvc.cache, None, config[TEST_SECTION])
        self.cloud = DataCloudAWS(cloud_settings)

    def test(self):
        if self._should_test():
            self._test_cloud()


class TestDataCloudGCP(TestDataCloudBase):
    TEST_REPO_GCP_BUCKET = 'dvc-test'

    def _should_test(self):
        if os.getenv("DVC_TEST_GCP") == "true":
            return True

        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.getenv("GCP_CREDS"):
            return True

        return False

    def _setup_cloud(self):
        if not self._should_test():
            return

        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.getenv("GCP_CREDS"):
            shutil.copyfile(self.GCP_CREDS_FILE, os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

        repo = 'gs://' + self.TEST_REPO_GCP_BUCKET + '/' + str(uuid.uuid4())

        # Setup cloud
        config = TEST_CONFIG
        config[TEST_SECTION][Config.SECTION_REMOTE_URL] = repo
        cloud_settings = CloudSettings(self.dvc.cache, None, config[TEST_SECTION])
        self.cloud = DataCloudGCP(cloud_settings)

    def test(self):
        if self._should_test():
            self._test_cloud()


class TestDataCloudLOCAL(TestDataCloudBase):
    def _should_test(self):
        return True

    def _setup_cloud(self):
        self.dname = 'cloud'
        os.mkdir(self.dname)

        config = TEST_CONFIG
        config[TEST_SECTION][Config.SECTION_REMOTE_URL] = self.dname
        cloud_settings = CloudSettings(self.dvc.cache, None, config[TEST_SECTION])
        self.cloud = DataCloudLOCAL(cloud_settings)

    def test(self):
        if self._should_test():
            self._test_cloud()
            self.assertTrue(os.path.isdir(self.dname))


class TestDataCloudLocalCli(TestDvc):
    def main(self, args):
        ret = main(args)
        self.assertEqual(ret, 0)

    def test(self):
        dname = 'cloud'
        os.mkdir(dname)

        self.main(['config', 'core.cloud', 'local'])
        self.main(['config', 'local.storagepath', dname])

        stage = self.dvc.add(self.FOO)
        cache = stage.outs[0].cache

        stage_dir = self.dvc.add(self.DATA_DIR)
        cache_dir = stage_dir.outs[0].cache

        #FIXME check status output
        self.main(['status'])

        self.main(['push'])
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        self.assertTrue(os.path.isfile(cache_dir))

        self.main(['status'])

        shutil.rmtree(self.dvc.cache.cache_dir)

        self.main(['status'])

        self.main(['pull'])
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        with open(cache, 'r') as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)
        self.assertTrue(os.path.isfile(cache_dir))

        self.main(['status'])


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

import os
import stat
import uuid
import shutil

from dvc.main import main
from dvc.config import ConfigError
from dvc.cloud.data_cloud import DataCloud, DataCloudAWS, DataCloudLOCAL, DataCloudGCP
from dvc.cloud.base import STATUS_UNKNOWN, STATUS_OK, STATUS_MODIFIED, STATUS_NEW, STATUS_DELETED
from dvc.cloud.instance_manager import CloudSettings

from tests.basic_env import TestDvc


class TestDataCloud(TestDvc):
    def test(self):
        for k,v in DataCloud.CLOUD_MAP.items():
            config = {'Global': {'Cloud': k},
                      k: {'StoragePath': 'a/b',
                          'ProjectName': 'name'}}
            cloud = DataCloud(self.dvc.cache, config)
            self.assertIsInstance(cloud._cloud, v)

        with self.assertRaises(ConfigError) as cx:
            config = {'Global': {'Cloud': 'not_supported_type'}}
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
        self.assertTrue(os.path.exists(cache_dir))
        self.assertTrue(os.path.isdir(cache_dir))

        status = self.cloud.status(cache)
        self.assertEqual(status, STATUS_OK)

        status_dir = self.cloud.status(cache_dir)
        self.assertEqual(status_dir, STATUS_OK)

        # Modify and check status
        os.chmod(cache, stat.S_IWRITE)
        with open(cache, 'a') as fd:
            fd.write('addon')
        os.chmod(cache, 0o777)
        status = self.cloud.status(cache)
        self.assertEqual(status, STATUS_MODIFIED)

        # Remove and check status
        os.unlink(cache)
        status = self.cloud.status(cache)
        self.assertEqual(status, STATUS_DELETED)

        self.dvc._remove_cache(cache_dir)
        status_dir = self.cloud.status(cache_dir)
        self.assertEqual(status_dir, STATUS_DELETED)

        # Pull and check status
        self.cloud.pull(cache)
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        with open(cache, 'r') as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)

        self.cloud.pull(cache_dir)
        self.assertTrue(os.path.exists(cache_dir))
        self.assertTrue(os.path.isdir(cache_dir))

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

        repo = self.TEST_REPO_BUCKET + '/' + str(uuid.uuid4())

        # Setup cloud
        config = {'StoragePath': repo,
                  'Region': self.TEST_REPO_REGION}
        cloud_settings = CloudSettings(self.dvc.cache, None, config)
        self.cloud = DataCloudAWS(cloud_settings)

    def test(self):
        if self._should_test():
            self._test_cloud()


class TestDataCloudGCP(TestDataCloudBase):
    TEST_REPO_GCP_BUCKET = 'dvc-test'
    TEST_REPO_GCP_PROJECT='dvc-project'

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

        repo = self.TEST_REPO_GCP_BUCKET + '/' + str(uuid.uuid4())

        # Setup cloud
        config = {'StoragePath': repo,
                  'Region': self.TEST_REPO_GCP_PROJECT}
        cloud_settings = CloudSettings(self.dvc.cache, None, config)
        self.cloud = DataCloudGCP(cloud_settings)

    def test(self):
        if self._should_test():
            self._test_cloud()


class TestDataCloudLOCAL(TestDataCloudBase):
    def _should_test(self):
        if os.name == 'nt':
            return False
        return True

    def _setup_cloud(self):
        self.dname = 'cloud'
        os.mkdir(self.dname)
        config = {'StoragePath': self.dname}
        cloud_settings = CloudSettings(self.dvc.cache, None, config)
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
        #FIXME enable this test for windows
        if os.name == 'nt':
            return
        
        dname = 'cloud'
        os.mkdir(dname)

        self.main(['config', 'global.cloud', 'LOCAL'])
        self.main(['config', 'LOCAL.StoragePath', dname])

        stage = self.dvc.add(self.FOO)
        cache = stage.outs[0].cache

        stage_dir = self.dvc.add(self.DATA_DIR)
        cache_dir = stage_dir.outs[0].cache

        #FIXME check status output
        self.main(['cache', 'status'])

        self.main(['cache', 'push'])
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        self.assertTrue(os.path.exists(cache_dir))
        self.assertTrue(os.path.isdir(cache_dir))

        self.main(['cache', 'status'])

        os.chmod(cache, 0o777)
        os.unlink(cache)
        self.dvc._remove_cache(cache_dir)

        self.main(['cache', 'status'])

        self.main(['cache', 'pull'])
        self.assertTrue(os.path.exists(cache))
        self.assertTrue(os.path.isfile(cache))
        with open(cache, 'r') as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)
        self.assertTrue(os.path.exists(cache_dir))
        self.assertTrue(os.path.isdir(cache_dir))

        self.main(['cache', 'status'])

import os
import uuid

from dvc.config import ConfigError
from dvc.data_cloud import DataCloud, DataCloudAWS, DataCloudLOCAL
from dvc.data_cloud import STATUS_UNKNOWN, STATUS_OK, STATUS_MODIFIED, STATUS_NEW, STATUS_DELETED
from dvc.cloud.instance_manager import CloudSettings

from tests.basic_env import TestDvc


class TestDataCloud(TestDvc):
    def test(self):
        for k,v in DataCloud.CLOUD_MAP.items():
            config = {'Global': {'Cloud': k},
                      k: {'StoragePath': 'a/b',
                          'ProjectName': 'name'}}
            cloud = DataCloud(config)
            self.assertIsInstance(cloud._cloud, v)

        with self.assertRaises(ConfigError) as cx:
            config = {'Global': {'Cloud': 'not_supported_type'}}
            DataCloud(config)


class TestDataCloudBase(TestDvc):
    def _setup_cloud(self):
        self.cloud = None

    def _test_cloud(self):
        self._setup_cloud()

        # Check status
        status = self.cloud.status(self.FOO)
        self.assertEqual(status, STATUS_NEW)

        # Push and check status
        self.cloud.push(self.FOO)
        self.assertTrue(os.path.exists(self.FOO))
        self.assertTrue(os.path.isfile(self.FOO))

        status = self.cloud.status(self.FOO)
        self.assertEqual(status, STATUS_OK)

        # Modify and check status
        with open(self.FOO, 'a') as fd:
            fd.write('addon')
        status = self.cloud.status(self.FOO)
        self.assertEqual(status, STATUS_MODIFIED)

        # Remove and check status
        os.unlink(self.FOO)
        status = self.cloud.status(self.FOO)
        self.assertEqual(status, STATUS_DELETED)

        # Pull and check status
        self.cloud.pull(self.FOO)
        self.assertTrue(os.path.exists(self.FOO))
        self.assertTrue(os.path.isfile(self.FOO))
        with open(self.FOO, 'r') as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)

        status = self.cloud.status(self.FOO)
        self.assertEqual(status, STATUS_OK)


class TestDataCloudAWS(TestDataCloudBase):
    TEST_REPO_BUCKET = 'dvc-test'
    TEST_REPO_REGION = 'us-east-2'

    def _should_test(self):
        if os.getenv("TRAVIS_PULL_REQUEST") == "false" and \
           os.getenv("TRAVIS_SECURE_ENV_VARS") == "true":
            return True
        return False

    def _setup_cloud(self):
        if not self._should_test():
            return

        repo = self.TEST_REPO_BUCKET + '/' + str(uuid.uuid4())

        # Setup cloud
        config = {'StoragePath': repo,
                  'Region': self.TEST_REPO_REGION}
        cloud_settings = CloudSettings(None, config)
        self.cloud = DataCloudAWS(cloud_settings)

    def test(self):
        if os.getenv("TRAVIS_PULL_REQUEST") == "false" and \
           os.getenv("TRAVIS_SECURE_ENV_VARS") == "true":
            self._test_cloud()


class TestDataCloudLOCAL(TestDataCloudBase):
    def _setup_cloud(self):
        self.dname = 'cloud'
        os.mkdir(self.dname)
        config = {'StoragePath': self.dname}
        cloud_settings = CloudSettings(None, config)
        self.cloud = DataCloudLOCAL(cloud_settings)

    def test(self):
        self._test_cloud()
        self.assertTrue(os.path.isdir(self.dname))
        path = os.path.join(self.dname, self.FOO)
        self.assertTrue(os.path.isfile(path))

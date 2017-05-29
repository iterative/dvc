import io
import os
import shutil
import StringIO
import traceback
from unittest import TestCase
import mock


from dvc.path.path import Path
from dvc.system import System
from dvc.config import Config
from dvc.command.data_cloud import DataCloud

class TestConfigTest(TestCase):
    def setUp(self):
        pass

    def storage_path_hierarchy_test(self):
        """ StoragePath should be read first from global and then from cloud section """

        c = ("[Global]",
             "LogLevel =",
             "DataDir = ",
             "CacheDir = ",
             "StateDir = ",
             "Cloud = aws",
             "StoragePath = globalsb/global_storage_path",
             "[AWS]",
             "StoragePath = awssb/aws_storage_path",
             "CredentialPath =",
             "[GCP]",
             "StoragePath = googlesb/google_storage_path"
           )
        s = StringIO.StringIO('\n'.join(c))
        conf = Config(s, conf_pseudo_file = s)
        cloud = DataCloud(conf)
        self.assertEqual(cloud.typ, 'aws')
        self.assertEqual(cloud._cloud.storage_bucket, 'globalsb')
        self.assertEqual(cloud._cloud.storage_prefix, 'global_storage_path')

        c = ("[Global]",
             "LogLevel =",
             "DataDir = ",
             "CacheDir = ",
             "StateDir = ",
             "Cloud = Aws",
             "[AWS]",
             "StoragePath = awssb/aws_storage_path",
             "CredentialPath =",
             "[GCP]",
             "StoragePath = googlesb/google_storage_path"
           )
        s = StringIO.StringIO('\n'.join(c))
        conf = Config(s, conf_pseudo_file=s)
        cloud = DataCloud(conf)
        self.assertEqual(cloud.typ, 'aws')
        self.assertEqual(cloud._cloud.storage_bucket, 'awssb')
        self.assertEqual(cloud._cloud.storage_prefix, 'aws_storage_path')


    def mocked_open_aws_default_credentials(klass, file, asdf):
        """ mocked open for ~/.aws/credentials """
        if file == os.path.expanduser('~/.aws/credentials'):
            creds0 = ("[default]",
                      "aws_access_key_id = default_access_id",
                      "aws_secret_access_key = default_sekret")
            return io.BytesIO('\n'.join(creds0))

        creds1 = ("[default]",
                  "aws_access_key_id = override_access_id",
                  "aws_secret_access_key = override_sekret")

        return io.BytesIO('\n'.join(creds1))

    def aws_credentials_default_test(self):
        """ in absence of [AWS] -> CredentialPath, aws creds should be read from ~/.aws/credentials """

        default_path = os.path.expanduser('~/.aws/credentials')
        c = ("[Global]",
             "LogLevel =",
             "DataDir = ",
             "CacheDir = ",
             "StateDir = ",
             "Cloud = aws",
             "[AWS]",
             "StoragePath = awssb/aws_storage_path",
             "CredentialPath =",
           )
        s = StringIO.StringIO('\n'.join(c))

        patcher = mock.patch('__builtin__.open', side_effect=self.mocked_open_aws_default_credentials)
        patcher.start()
        conf = Config(s, conf_pseudo_file=s)
        cloud = DataCloud(conf)
        aws_creds = cloud._cloud.get_aws_credentials()
        patcher.stop()

        self.assertEqual(aws_creds[0], 'default_access_id')
        self.assertEqual(aws_creds[1], 'default_sekret')


    def mocked_isfile(file):
        """ tell os some_credential_path exists """
        return file == 'some_credential_path'

    @mock.patch('dvc.config.os.path.isfile', side_effect = mocked_isfile)
    def aws_credentials_specified_test(self, isfile_function):
        """ in presence of [AWS] -> CredentialPath, use those credentials """

        c = ("[Global]",
             "LogLevel =",
             "DataDir = ",
             "CacheDir = ",
             "StateDir = ",
             "Cloud = aws",
             "[AWS]",
             "StoragePath = awssb/aws_storage_path",
             "CredentialPath = some_credential_path",
           )
        s = StringIO.StringIO('\n'.join(c))

        patcher = mock.patch('__builtin__.open', side_effect=self.mocked_open_aws_default_credentials)
        patcher.start()
        conf = Config(s, conf_pseudo_file=s)
        cloud = DataCloud(conf)
        aws_creds = cloud._cloud.get_aws_credentials()
        patcher.stop()

        self.assertEqual(aws_creds[0], 'override_access_id')
        self.assertEqual(aws_creds[1], 'override_sekret')

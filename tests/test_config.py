import os
from unittest import TestCase
import mock

try:
    from StringIO import StringIO
except ImportError:
    # Python 3
    from io import StringIO

try:
    import __builtin__
    builtin_module_name = '__builtin__'
except ImportError:
    # Python 3
    builtin_module_name = 'builtins'

from dvc.config import Config
from dvc.data_cloud import DataCloud
from dvc.settings import Settings


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
        s = StringIO('\n'.join(c))
        conf = Config(s, conf_pseudo_file = s)
        settings = Settings(None, None, conf)
        cloud = DataCloud(settings)
        self.assertEqual(cloud.typ, 'AWS')
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
        s = StringIO('\n'.join(c))
        conf = Config(s, conf_pseudo_file=s)
        cloud = DataCloud(Settings(None, None, conf))
        self.assertEqual(cloud.typ, 'AWS')
        self.assertEqual(cloud._cloud.storage_bucket, 'awssb')
        self.assertEqual(cloud._cloud.storage_prefix, 'aws_storage_path')

    def mocked_open_aws_default_credentials(klass, file, asdf):
        """ mocked open for ~/.aws/credentials """
        if file == os.path.expanduser('~/.aws/credentials'):
            creds0 = ("[default]",
                      "aws_access_key_id = default_access_id",
                      "aws_secret_access_key = default_sekret")
            return StringIO('\n'.join(creds0))

        creds1 = ("[default]",
                  "aws_access_key_id = override_access_id",
                  "aws_secret_access_key = override_sekret")

        return StringIO('\n'.join(creds1))

    def aws_credentials_default_tes_t_DISABLED(self):
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
        s = StringIO('\n'.join(c))

        patcher = mock.patch(builtin_module_name + '.open', side_effect=self.mocked_open_aws_default_credentials)

        # patcher.start()
        conf = Config(s, conf_pseudo_file=s)
        cloud = DataCloud(Settings(None, None, conf))
        aws_creds = cloud._cloud._get_credentials()
        patcher.stop()

        self.assertEqual(aws_creds[0], 'default_access_id')
        self.assertEqual(aws_creds[1], 'default_sekret')

    def mocked_isfile(file):
        """ tell os some_credential_path exists """
        return file == 'some_credential_path'

    @mock.patch('dvc.config.os.path.isfile', side_effect = mocked_isfile)
    def aws_credentials_specified_tes_t_DISABLED(self, isfile_function):
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
        s = StringIO('\n'.join(c))

        patcher = mock.patch(builtin_module_name + '.open', side_effect=self.mocked_open_aws_default_credentials)

        patcher.start()
        conf = Config(s, conf_pseudo_file=s)
        cloud = DataCloud(Settings(None, None, conf))
        aws_creds = cloud._cloud._get_credentials()
        patcher.stop()

        self.assertEqual(aws_creds[0], 'override_access_id')
        self.assertEqual(aws_creds[1], 'override_sekret')

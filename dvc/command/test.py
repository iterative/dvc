import os
import sys

from pathlib import Path

from google.cloud import storage

from dvc.command.base import CmdBase
from dvc.logger import Logger
from dvc.config import Config
from dvc.runtime import Runtime

from dvc.settings import SettingsError



class CmdTest(CmdBase):
    def __init__(self, settings):
        super(CmdTest, self).__init__(settings)

    def define_args(self, parser):
        self.set_no_git_actions(parser)

    def run(self):

        cloud = self.settings._config.cloud
        assert cloud in ['amazon', 'google'], 'unknown cloud: %s' % cloud
        if cloud == 'amazon':
            print('TODO')
            sys.exit(-1)
        elif cloud == 'google':
            Logger.info('testing gcloud')

            if self.settings._config.gc_project_name == '':
                Logger.error('Please specify the google cloud project name in dvc.conf')
                raise SettingsError('must specify GC ProjectName')

            try:
                client = storage.Client(project=self.settings._config.gc_project_name)
            except:
                Logger.error('initializing storage.Client raised')
                raise

            sb = self.config.storage_bucket
            if sb == '':
                Logger.error('Please specificy a bucket for google cloud in dvc.conf')
                sys.exit(-1)

            bucket = client.bucket(sb)
            if not bucket.exists():
                Logger.error('google cloud storage: bucket %s doesn\'t exist' % sb)
                sys.exit(-1)
            Logger.info('bucket %s visible: google cloud seems to be configured correctly' % sb)
            sys.exit(0)


if __name__ == '__main__':
    Runtime.run(CmdTest, False)

"""
DVC config objects.
"""
import os
import configparser

from dvc.exceptions import DvcException


class ConfigError(DvcException):
    """ DVC config exception """
    def __init__(self, msg):
        DvcException.__init__(self, 'Config file error: {}'.format(msg))


class Config(object):
    CONFIG = 'config'
    SECTION_CORE = 'Core'
    CONFIG_TEMPLATE = '''
[Core]
# Supported clouds: AWS, GCP
Cloud = AWS

# Log levels: Debug, Info, Warning and Error
LogLevel = Info

[AWS]
CredentialPath = ~/.aws/credentials
Profile = default

StoragePath = dvc/tutorial

[GCP]
StoragePath = 
ProjectName = 

'''

    def __init__(self, dvc_dir):
        self.dvc_dir = os.path.abspath(os.path.realpath(dvc_dir))
        self.config_file = os.path.join(dvc_dir, self.CONFIG)

        self._config = configparser.SafeConfigParser()

        try:
            self._config.read(self.config_file)
        except configparser.Error as ex:
            raise ConfigError(ex.message)

        if not self._config.has_section(self.SECTION_CORE):
            raise ConfigError(u'section \'{}\' was not found'.format(self.SECTION_CORE))

    @staticmethod
    def init(dvc_dir):
        config_file = os.path.join(dvc_dir, Config.CONFIG)
        open(config_file, 'w').write(Config.CONFIG_TEMPLATE)
        return Config(dvc_dir)

"""
DVC config objects.
"""
import os
import configparser

from dvc.exceptions import DvcException
from dvc.logger import Logger


class ConfigError(DvcException):
    """ DVC config exception """
    def __init__(self, msg):
        DvcException.__init__(self, 'Config file error: {}'.format(msg))


class Config(object):
    CONFIG = 'config'
    CONFIG_TEMPLATE = '''
[Global]
# Supported clouds: AWS, GCP
Cloud = AWS

# Log levels: Debug, Info, Warning and Error
LogLevel = Info

[AWS]
CredentialPath = ~/.aws/credentials
CredentialSection = default

StoragePath = dvc/tutorial

# Default settings for AWS instances:
Type = t2.nano
Image = ami-2d39803a

SpotPrice = 
SpotTimeout = 300

KeyPairName = dvc-key
KeyPairDir = ~/.ssh
SecurityGroup = dvc-sg

Region = us-east-1
Zone = us-east-1a
SubnetId = 

Volume = my-100gb-drive-io

Monitoring = false
EbsOptimized = false
AllDisksAsRAID0 = false

[GCP]
StoragePath = 
ProjectName = 
'''

    def __init__(self, dvc_dir):
        self.dvc_dir = dvc_dir
        self.config_file = os.path.join(dvc_dir, self.CONFIG)
        
        self._config = configparser.SafeConfigParser()
        self._config.read(self.config_file)
        Logger.set_level(self._config['Global']['LogLevel'])

    @staticmethod
    def init(dvc_dir):
        config_file = os.path.join(dvc_dir, Config.CONFIG)
        open(config_file, 'w').write(Config.CONFIG_TEMPLATE)
        return Config(dvc_dir)

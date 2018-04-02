"""
DVC config objects.
"""
import os
import schema
import configobj

from dvc.exceptions import DvcException


class ConfigError(DvcException):
    """ DVC config exception """
    def __init__(self, msg):
        DvcException.__init__(self, 'Config file error: {}'.format(msg))


class Config(object):
    CONFIG = 'config'

    SECTION_CORE = 'core'
    SECTION_CORE_LOGLEVEL = 'loglevel'
    SECTION_CORE_LOGLEVEL_SCHEMA = schema.And(schema.Use(str.lower), lambda l: l in ('info', 'debug', 'warning', 'error'))
    SECTION_CORE_CLOUD = 'cloud'
    SECTION_CORE_CLOUD_SCHEMA = schema.And(schema.Use(str.lower), lambda c: c in ('aws', 'gcp', 'local', ''))
    SECTION_CORE_STORAGEPATH = 'storagepath' # backward compatibility
    SECTION_CORE_SCHEMA = {
        schema.Optional(SECTION_CORE_LOGLEVEL, default='info'): SECTION_CORE_LOGLEVEL_SCHEMA,
        schema.Optional(SECTION_CORE_CLOUD, default=''): SECTION_CORE_CLOUD_SCHEMA,
        schema.Optional(SECTION_CORE_STORAGEPATH, default=''): str,
    }

    SECTION_AWS = 'aws'
    SECTION_AWS_STORAGEPATH = 'storagepath'
    SECTION_AWS_CREDENTIALPATH = 'credentialpath'
    SECTION_AWS_REGION = 'region'
    SECTION_AWS_PROFILE = 'profile'
    SECTION_AWS_SCHEMA = {
        SECTION_AWS_STORAGEPATH: str,
        schema.Optional(SECTION_AWS_REGION): str,
        schema.Optional(SECTION_AWS_PROFILE, default='default'): str,
        schema.Optional(SECTION_AWS_CREDENTIALPATH, default = ''): str,
    }

    SECTION_GCP = 'gcp'
    SECTION_GCP_STORAGEPATH = SECTION_AWS_STORAGEPATH
    SECTION_GCP_PROJECTNAME = 'projectname'
    SECTION_GCP_SCHEMA = {
        SECTION_GCP_STORAGEPATH: str,
        SECTION_GCP_PROJECTNAME: str,
    }

    SECTION_LOCAL = 'local'
    SECTION_LOCAL_STORAGEPATH = SECTION_AWS_STORAGEPATH
    SECTION_LOCAL_SCHEMA = {
        SECTION_LOCAL_STORAGEPATH: str,
    }

    SCHEMA = {
        schema.Optional(SECTION_CORE, default={}): SECTION_CORE_SCHEMA,
        schema.Optional(SECTION_AWS, default={}): SECTION_AWS_SCHEMA,
        schema.Optional(SECTION_GCP, default={}): SECTION_GCP_SCHEMA,
        schema.Optional(SECTION_LOCAL, default={}): SECTION_LOCAL_SCHEMA,
    }

    def __init__(self, dvc_dir):
        self.dvc_dir = os.path.abspath(os.path.realpath(dvc_dir))
        self.config_file = os.path.join(dvc_dir, self.CONFIG)

        try:
            self._config = configobj.ConfigObj(self.config_file, write_empty_values=True)
            self._config = self._lower(self._config)
            self._config = schema.Schema(self.SCHEMA).validate(self._config)
        except Exception as ex:
            raise ConfigError(ex.message)

    @staticmethod
    def _lower(config):
        new_config = {}
        for s_key, s_value in config.items():
            new_s = {}
            for key, value in s_value.items():
                new_s[key.lower()] = value
            new_config[s_key.lower()] = new_s
        return new_config

    @staticmethod
    def init(dvc_dir):
        config_file = os.path.join(dvc_dir, Config.CONFIG)
        open(config_file, 'w+').close()
        return Config(dvc_dir)

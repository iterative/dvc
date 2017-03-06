import argparse
import os

from neatlynx.git_wrapper import GitWrapper
from neatlynx.config import Config, ConfigError


class Logger(object):
    @staticmethod
    def info(msg):
        print('{}'.format(msg))

    @staticmethod
    def warn(msg):
        print('{}'.format(msg))

    @staticmethod
    def error(msg):
        print('{}'.format(msg))

    @staticmethod
    def verbose(msg):
        print('{}'.format(msg))


class CmdBase(object):
    CONFIG = 'neatlynx.conf'

    def __init__(self, parse_config=True):
        self._git = GitWrapper()
        self._args = None
        self._lnx_home = None

        self._config = None
        if parse_config:
            self._config = Config(os.path.join(self.git.git_dir, self.CONFIG))

        parser = argparse.ArgumentParser()
        self.define_args(parser)
        self._args = parser.parse_args()

        self._lnx_home = os.environ.get('NEATLYNX_HOME')

        if not self.lnx_home:
            raise ConfigError('NEATLYNX_HOME environment variable is not defined')
        if not os.path.exists(self._lnx_home):
            raise ConfigError("NEATLYNX_HOME directory doesn't exists")
        pass

    @property
    def config(self):
        return self._config

    @property
    def lnx_home(self):
        return self._lnx_home

    @property
    def args(self):
        return self._args

    @property
    def git(self):
        return self._git

    def define_args(self):
        pass

    def add_string_arg(self, parser, name, message, default = None,
                       conf_section=None, conf_name=None):
        if conf_section and conf_name:
            section = self.config[conf_section]
            if not section:
                raise ConfigError("")
            default_value = section.get(conf_section, default)
        else:
            default_value = default

        parser.add_argument(name,
                            metavar='',
                            default=default_value,
                            help=message)

    def run(self):
        pass
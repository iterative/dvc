import configparser
import argparse
import os
import git
import subprocess

from neatlynx.exceptions import GitCmdError, ConfigError

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


class BaseCmd(object):
    CONFIG = 'neatlynx.conf'

    def __init__(self, parse_config=True):
        self._git_dir = None
        self._args = None
        self._lnx_home = None

        if parse_config:
            self._config = configparser.ConfigParser()
            self._config.read(os.path.join(self.git_dir, self.CONFIG))

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
    def lnx_home(self):
        return self._lnx_home

    @property
    def args(self):
        return self._args

    @property
    def git_dir(self):
        if self._git_dir:
            return self._git_dir

        try:
            p = subprocess.Popen(['git', 'rev-parse', '--show-toplevel'],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            out, err = map(lambda s: s.decode().strip('\n\r'), p.communicate())

            if p.returncode != 0:
                raise GitCmdError('Git command error - {}'.format(err))

            self._git_dir = out
            return self._git_dir
        except GitCmdError as e:
            raise
        except Exception as e:
            raise GitCmdError('Unable to run git command: {}'.format(e))
        pass

    def define_args(self):
        pass

    def add_string_arg(self, parser, name, default, message,
                       conf_section=None, conf_name=None):
        if conf_section and conf_name:
            section = self._config[conf_section]
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
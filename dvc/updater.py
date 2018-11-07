import sys
import os
import time
import requests
import colorama

from dvc import VERSION_BASE
from dvc.logger import Logger


class Updater(object):  # pragma: no cover
    URL = 'https://4ki8820rsf.execute-api.us-east-2.amazonaws.com/' \
          'prod/latest-version'
    UPDATER_FILE = 'updater'
    TIMEOUT = 24 * 60 * 60  # every day
    TIMEOUT_GET = 10

    def __init__(self, dvc_dir):
        self.dvc_dir = dvc_dir
        self.updater_file = os.path.join(dvc_dir, self.UPDATER_FILE)
        self.current = VERSION_BASE

    def check(self):
        if os.getenv('CI'):
            return

        if os.path.isfile(self.updater_file):
            ctime = os.path.getctime(self.updater_file)
            if time.time() - ctime < self.TIMEOUT:
                msg = '{} is not old enough to check for updates'
                Logger.debug(msg.format(self.UPDATER_FILE))
                return

            os.unlink(self.updater_file)

        Logger.info('Checking for updates...')

        try:
            self._get_latest_version()
        except Exception as exc:
            msg = 'Failed to obtain latest version: {}'.format(str(exc))
            Logger.debug(msg)
            return

        if self._is_outdated():
            self._notify()

    def _get_latest_version(self):
        r = requests.get(self.URL, timeout=self.TIMEOUT_GET)
        j = r.json()
        self.latest = j['version']
        open(self.updater_file, 'w+').close()

    def _is_outdated(self):
        l_major, l_minor, l_patch = [int(x) for x in self.latest.split('.')]
        c_major, c_minor, c_patch = [int(x) for x in self.current.split('.')]

        return (l_major > c_major or
                l_minor > c_minor or
                l_patch > c_patch)

    def _notify(self):
        message = (
            'Update available {red}{current}{reset} -> {green}{latest}{reset}'
            + '\n'
            + self._get_update_instructions()
        ).format(red=colorama.Fore.RED,
                 reset=colorama.Fore.RESET,
                 green=colorama.Fore.GREEN,
                 yellow=colorama.Fore.YELLOW,
                 blue=colorama.Fore.BLUE,
                 current=self.current,
                 latest=self.latest)

        if sys.stdout.isatty():
            Logger.box(message, border_color='yellow')

    def _get_update_instructions(self):
        instructions = {
            'pip': 'Run {yellow}pip{reset} install dvc {blue}--upgrade{reset}',
            'yum': 'Run {yellow}yum{reset} update dvc',
            'yay': 'Run {yellow}yay{reset} {blue}-S{reset} dvc',
            'formula': 'Run {yellow}brew{reset} upgrade dvc',
            'cask': 'Run {yellow}brew cask{reset} upgrade dvc',
            'apt': ('Run {yellow}apt-get{reset} install'
                    ' {blue}--only-upgrade{reset} dvc'),
            'binary': ('To upgrade follow this steps:\n'
                       '1. Uninstall dvc binary\n'
                       '2. Go to {blue}https://dvc.org{reset}\n'
                       '3. Download and install new binary'),
            None: ('Find the latest release at\n{blue}'
                   'https://github.com/iterative/dvc/releases/latest'
                   '{reset}'),
        }

        package_manager = self._get_package_manager()

        return instructions[package_manager]

    @staticmethod
    def _is_binary():
        return getattr(sys, 'frozen', False)

    def _get_linux(self):
        import distro

        if not self._is_binary():
            return 'pip'

        package_managers = {
            'rhel':     'yum',
            'centos':   'yum',
            'fedora':   'yum',
            'amazon':   'yum',
            'opensuse': 'yum',
            'ubuntu':   'apt',
            'debian':   'apt',
        }

        return package_managers.get(distro.id())

    def _get_darwin(self):
        if not self._is_binary():
            if __file__.startswith('/usr/local/Cellar'):
                return 'formula'
            else:
                return 'pip'

        # NOTE: both pkg and cask put dvc binary into /usr/local/bin,
        # so in order to know which method of installation was used,
        # we need to actually call `brew cask`
        ret = os.system('brew cask ls dvc')
        if ret == 0:
            return 'cask'

        return None

    def _get_windows(self):
        return None if self._is_binary() else 'pip'

    def _get_package_manager(self):
        import platform
        from dvc.exceptions import DvcException

        m = {
            'Windows': self._get_windows,
            'Darwin': self._get_darwin,
            'Linux': self._get_linux,
        }

        system = platform.system()
        func = m.get(system)
        if func is None:
            raise DvcException("Not supported system '{}'".format(system))

        return func()

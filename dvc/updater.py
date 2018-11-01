import sys
import os
import time
import requests
import colorama
import distro
import subprocess

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
            'yum': 'Run {yellow}yum{reset} update {blue}dvc{reset}',
            'yay': 'Run {yellow}yay{reset} {blue}-S{reset} dvc',
            'brew': 'Run {yellow}brew{reset} upgrade {blue}dvc{reset}',
            'apt': ('Run {yellow}apt-get{reset} install'
                    ' {blue}--only-upgrade{reset} dvc'),
            None: ('To upgrade follow this steps:\n'
                   '1. Uninstall dvc binary\n'
                   '2. Go to {blue}https://dvc.org{reset}\n'
                   '3. Download and install new binary'),
        }

        package_manager = self._get_package_manager()

        return instructions[package_manager]

    def _get_package_manager(self):
        package_managers = {
            'rhel': 'yum',
            'centos': 'yum',
            'fedora': 'yum',
            'amazon': 'yum',
            'opensuse': 'yum',
            'arch': 'yay',
            'ubuntu': 'apt',
            'debian': 'apt',
            'darwin': 'brew',
            'windows': None,
        }

        if self._is_installed_with_pip():
            return 'pip'

        return package_managers[distro.id()]

    def _is_installed_with_pip(self):
        command = ['pip', 'show', 'dvc']

        try:
            with open(os.devnull, 'w') as devnull:
                return subprocess.check_call(command, stdout=devnull) == 0
        except Exception:
            return False

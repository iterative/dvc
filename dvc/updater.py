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

    @staticmethod
    def init(dvc_dir):
        return Updater(dvc_dir)

    def check(self):
        current = VERSION_BASE

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
            r = requests.get(self.URL, timeout=self.TIMEOUT_GET)
            j = r.json()
            latest = j['version']
            open(self.updater_file, 'w+').close()
        except Exception as exc:
            msg = 'Failed to obtain latest version: {}'.format(str(exc))
            Logger.debug(msg)
            return

        l_major, l_minor, l_patch = [int(x) for x in latest.split('.')]
        c_major, c_minor, c_patch = [int(x) for x in current.split('.')]

        if l_major <= c_major and \
           l_minor <= c_minor and \
           l_patch <= c_patch:
            return

        msg = (
            'Update available {red}{current}{reset} → {green}{latest}{reset}\n'
            'Run {yellow}pip{reset} install dvc {blue}--upgrade{reset}'
            .format(
                red=colorama.Fore.RED,
                reset=colorama.Fore.RESET,
                green=colorama.Fore.GREEN,
                yellow=colorama.Fore.YELLOW,
                blue=colorama.Fore.BLUE,
                current=current,
                latest=latest,
            )
        )

        Logger.box(msg, border_color='yellow')

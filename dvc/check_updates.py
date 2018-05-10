import os
import time
import json
import requests

from dvc import VERSION_BASE
from dvc.logger import Logger

DVCAPI_URL = 'https://4ki8820rsf.execute-api.us-east-2.amazonaws.com/prod/latest-version'
CHECK_UPDATES_FILE = '.check_updates'
TIMEOUT = 7 * 24 * 60 * 60 #every week

def check_updates(dvc_dir):
    current = VERSION_BASE

    if os.getenv('CI'):
        return

    fname = os.path.join(dvc_dir, CHECK_UPDATES_FILE)
    if os.path.isfile(fname):
        ctime = os.path.getctime(fname)
        if time.time() - ctime < TIMEOUT:
            msg = '{} is not old enough to check for updates'
            Logger.debug(msg.format(CHECK_UPDATES_FILE))
            return

        os.unlink(fname)

    try:
        r = requests.get(DVCAPI_URL)
        j = json.loads(r.content)
        latest = j['version']
        open(fname, 'w+').close()
    except Exception as exc:
        Logger.debug('Failed to obtain latest version: {}'.format(str(exc)))
        return

    l_major, l_minor, l_patch = latest.split('.')
    c_major, c_minor, c_patch = current.split('.')

    if l_major <= c_major and \
       l_minor <= c_minor and \
       l_patch <= c_patch:
           return

    msg = 'You are using dvc version {}, however version {} is available. Consider upgrading.'
    Logger.warn(msg.format(current, latest))

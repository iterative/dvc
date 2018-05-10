import json
import requests

from dvc import VERSION_BASE
from dvc.logger import Logger

DVCAPI_URL = 'https://4ki8820rsf.execute-api.us-east-2.amazonaws.com/prod/latest-version'

def check_updates():
    current = VERSION_BASE

    try:
        r = requests.get(DVCAPI_URL)
        j = json.loads(r.content)
        latest = j['version']
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

from github import Github

from dvc import VERSION_BASE
from dvc.logger import Logger

GITHUB_USER = 'dataversioncontrol'
GITHUB_REPO = 'dvc'

def check_updates():
    current = VERSION_BASE

    try:
        gh = Github()
        user = gh.get_user(GITHUB_USER)
        repo = user.get_repo(GITHUB_REPO)
        latest = repo.get_latest_release().title
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

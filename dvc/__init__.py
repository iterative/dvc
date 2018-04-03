"""
DVC
----
Make your data science projects reproducible and shareable.
"""
import os

VERSION = '0.9.4'

if os.getenv('APPVEYOR_REPO_TAG', '').lower() != 'true' and os.getenv('TRAVIS_TAG', '') == '':
    # Dynamically update version
    try:
        import git
        repo = git.Repo(os.curdir, search_parent_directories=True)
        sha = repo.head.object.hexsha
        short_sha = repo.git.rev_parse(sha, short=6)
        dirty = '.mod' if repo.is_dirty() else ''
        VERSION = '{}+{}{}'.format(VERSION, short_sha, dirty)
    except:
        pass

__version__ = VERSION

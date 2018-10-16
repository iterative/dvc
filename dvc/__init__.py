"""
DVC
----
Make your data science projects reproducible and shareable.
"""
import os
import warnings


VERSION_BASE = '0.19.12'
__version__ = VERSION_BASE

PACKAGEPATH = os.path.abspath(os.path.dirname(__file__))
HOMEPATH = os.path.dirname(PACKAGEPATH)
VERSIONPATH = os.path.join(PACKAGEPATH, 'version.py')

if os.path.exists(os.path.join(HOMEPATH, 'setup.py')):
    # dvc is run directly from source without installation or
    # __version__ is called from setup.py
    if os.getenv('APPVEYOR_REPO_TAG', '').lower() != 'true' \
       and os.getenv('TRAVIS_TAG', '') == '':
        # Dynamically update version
        try:
            import git
            repo = git.Repo(HOMEPATH)
            sha = repo.head.object.hexsha
            short_sha = repo.git.rev_parse(sha, short=6)
            dirty = '.mod' if repo.is_dirty() else ''
            __version__ = '{}+{}{}'.format(__version__, short_sha, dirty)

            # Write a helper file, that will be installed with the package
            # and will provide a true version of the installed dvc
            with open(VERSIONPATH, 'w+') as fd:
                fd.write('# AUTOGENERATED by dvc/__init__.py\n')
                fd.write('version = "{}"\n'.format(__version__))
        except Exception:  # pragma: no cover
            pass
    else:  # pragma: no cover
        # Remove version.py so that it doesn't get into the release
        if os.path.exists(VERSIONPATH):
            os.unlink(VERSIONPATH)
else:  # pragma: no cover
    # dvc was installed with pip or something. Hopefully we have our
    # auto-generated version.py to help us provide a true version
    try:
        from dvc.version import version
        __version__ = version
    except Exception:
        pass

VERSION = __version__


# Ignore numpy's runtime warnings: https://github.com/numpy/numpy/pull/432.
# We don't directly import numpy, but our dependency networkx does, causing
# these warnings in some environments. Luckily these warnings are benign and
# we can simply ignore them so that they don't show up when you are using dvc.
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

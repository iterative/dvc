import os
import stat
import shutil

from dvc.exceptions import DvcException
from dvc.logger import Logger


def cached_property(f):
    def get(self):
        try:
            return self._property_cache[f]
        except AttributeError:
            self._property_cache = {}
            x = self._property_cache[f] = f(self)
            return x
        except KeyError:
            x = self._property_cache[f] = f(self)
            return x

    return property(get)


def rmtree(dir):
    '''Cross platform rmtree()'''
    if os.name == 'nt':
        if os.path.exists(dir) and not os.access(dir, os.W_OK):
            os.chmod(dir, stat.S_IWUSR)
        else:
            raise DvcException('Windows rmtree() error')

    before = os.path.exists(dir)

    if os.path.exists(dir) and os.path.islink(dir):
        os.unlink(dir)
    else:
        # shutil.rmtree(dir, ignore_errors=True)
        shutil.rmtree(dir)
        # os.remove(dir)

    after = os.path.exists(dir)
    Logger.error('$$$$$$$$$$$$$$$$$$ rmtree(%s) before=%s, after=%s' % (dir, before, after))


def rmfile(file):
    os.remove(file)

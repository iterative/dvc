import os
import stat
import shutil

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

    shutil.rmtree(dir, ignore_errors=True)

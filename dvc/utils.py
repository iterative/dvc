from dvc.exceptions import NeatLynxException
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


def run(cmd):
    import sys
    try:
        sys.exit(cmd.run())
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)
def import_string(import_name, silent=False):
    """Imports an object based on a string.

    Useful to delay import to not load everything on startup.
    Use dotted notaion in `import_name`, e.g. 'dvc.remote.gs.RemoteGS'.
    If the `silent` is True the return value will be `None` if the import
    fails.

    :return: imported object
    """
    try:
        if "." in import_name:
            module, obj = import_name.rsplit(".", 1)
        else:
            return __import__(import_name)
        return getattr(__import__(module, None, None, [obj]), obj)
    except (ImportError, AttributeError):
        if not silent:
            raise


class LazyObject(object):
    """
    A simplistic lazy init object.
    Rewrites itself when any attribute is accesssed.
    """

    def __init__(self, init):
        self.__dict__["_LazyObject_init"] = init

    def _setup(self):
        obj = self._LazyObject_init()
        try:
            object.__setattr__(self, "__class__", obj.__class__)
        except TypeError:
            pass
        object.__setattr__(self, "__dict__", obj.__dict__)

    def __getattr__(self, name):
        self._setup()
        return getattr(self, name)

    def __setattr__(self, name, value):
        self._setup()
        return setattr(self, name, value)


def lazy_import(import_name, silent=False):
    return LazyObject(lambda: import_string(import_name, silent=silent))

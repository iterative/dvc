import threading

from funcy import cached_property, wrap_prop

from dvc.scheme import Schemes

from .fsspec_wrapper import FSSpecWrapper


class MemoryFileSystem(FSSpecWrapper):  # pylint:disable=abstract-method
    scheme = Schemes.MEMORY
    PARAM_CHECKSUM = "md5"
    TRAVERSE_PREFIX_LEN = 2
    DEFAULT_BLOCKSIZE = 4096

    def __eq__(self, other):
        # NOTE: all fsspec MemoryFileSystem instances are equivalent and use a
        # single global store
        return isinstance(other, type(self))

    __hash__ = FSSpecWrapper.__hash__

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from fsspec.implementations.memory import MemoryFileSystem as MemFS

        return MemFS(**self.fs_args)

    def open(self, *args, **kwargs):
        with super().open(*args, **kwargs) as fobj:
            fobj.blocksize = self.DEFAULT_BLOCKSIZE
            return fobj

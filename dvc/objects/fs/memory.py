import threading

from funcy import cached_property, wrap_prop

from dvc.scheme import Schemes

from .base import FileSystem


class MemoryFileSystem(FileSystem):  # pylint:disable=abstract-method
    scheme = Schemes.MEMORY
    PARAM_CHECKSUM = "md5"

    def __eq__(self, other):
        # NOTE: all fsspec MemoryFileSystem instances are equivalent and use a
        # single global store
        return isinstance(other, type(self))

    __hash__ = FileSystem.__hash__

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from fsspec.implementations.memory import MemoryFileSystem as MemFS

        return MemFS(**self.fs_args)

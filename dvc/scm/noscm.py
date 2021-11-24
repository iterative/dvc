from .base import Base


# Syntactic sugar to signal that this is an actual implementation for a DVC
# project under no SCM control.
class NoSCM(Base):
    def __init__(
        self,
        root_dir: str = None,
        _raise_not_implemented_as=NotImplementedError,
    ):
        super().__init__(root_dir=root_dir)
        self._exc = _raise_not_implemented_as

    def __getattr__(self, name):
        raise self._exc

import locale
import pathlib
from abc import ABC, abstractmethod


class Cloud(ABC):
    IS_OBJECT_STORAGE = False

    @abstractmethod
    def is_file(self):
        pass

    @abstractmethod
    def is_dir(self):
        pass

    @abstractmethod
    def exists(self):
        pass

    @abstractmethod
    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        pass

    def write_text(self, contents, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        self.write_bytes(contents.encode(encoding))

    @abstractmethod
    def write_bytes(self, contents):
        raise NotImplementedError

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)

    @abstractmethod
    def read_bytes(self):
        pass

    def _gen(self, struct, prefix=None):
        for name, contents in struct.items():
            path = (prefix or self) / name

            if isinstance(contents, dict):
                if not contents:
                    path.mkdir(parents=True, exist_ok=True)
                else:
                    self._gen(contents, prefix=path)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                if isinstance(contents, bytes):
                    path.write_bytes(contents)
                else:
                    path.write_text(contents, encoding="utf-8")

    def gen(self, struct, text=""):
        if isinstance(struct, (str, bytes, pathlib.PurePath)):
            struct = {struct: text}

        self._gen(struct)
        return struct.keys()

    def close(self):  # noqa: B027
        pass

    @staticmethod
    def should_test():
        return True

    @staticmethod
    def get_url():
        raise NotImplementedError

    @property
    @abstractmethod
    def config(self):
        pass

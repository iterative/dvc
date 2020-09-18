import os

from funcy import get_in

from dvc.config import merge as _merge
from dvc.utils.serialize import LOADERS


def _key_parts(key):
    return key.split(sep=".")


class Context(dict):
    @classmethod
    def load(cls, file: str):
        # supports yaml1.2, toml and json
        d = LOADERS[os.path.splitext(file)[1]](file)
        # TODO: what to do for the scalar values?
        return cls(d or {})

    @classmethod
    def load_and_select(cls, file, key=None):
        ctx = cls.load(file)
        return ctx.select(key) if key else ctx

    @staticmethod
    def merge(*args):
        assert len(args) >= 2
        m_dct, *rems = args
        for d in rems:
            _merge(m_dct, d)
        return m_dct

    def update(self, key, value):
        *parts, to_replace = _key_parts(key)
        d = self
        for part in parts:
            d = d[part]

        d[to_replace] = value
        return self

    def select(self, key: str):
        d = get_in(self, _key_parts(key))
        # TODO: what to do for the scalar values?
        assert isinstance(d, (Context, dict))
        return Context(d) if isinstance(d, dict) else d

# pylint: disable=unused-import
try:
    from ._version import version as __version__
    from ._version import version_tuple  # noqa: F401
except ImportError:
    __version__ = "UNKNOWN"

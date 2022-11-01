# pylint: disable=unused-import
try:
    from ._dvc_version import version as __version__
    from ._dvc_version import version_tuple  # noqa: F401
except ImportError:
    __version__ = "UNKNOWN"
    version_tuple = (0, 0, __version__)  # type: ignore[assignment]

try:
    from ._version import version as __version__  # type: ignore[import]
    from ._version import version_tuple  # type: ignore[import]
except ImportError:
    __version__ = "UNKNOWN"
    version_tuple = (0, 0, __version__)  # type: ignore[assignment]

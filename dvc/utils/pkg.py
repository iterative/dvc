try:
    # file is created during dvc build
    # pylint:disable=unused-import
    from .build import PKG  # type: ignore[import]
except ImportError:
    PKG = None  # type: ignore[assignment]

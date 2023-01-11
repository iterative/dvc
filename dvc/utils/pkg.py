try:
    # file is created during dvc build
    from .build import PKG  # pylint:disable=unused-import
except ImportError:
    PKG = None  # type: ignore[assignment]

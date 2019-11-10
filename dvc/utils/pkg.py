try:
    from .build import PKG  # file created during dvc build
except ImportError:
    PKG = None

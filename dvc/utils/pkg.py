def get_package_manager():
    try:
        from .build import PKG  # file created during dvc build

        return PKG
    except ImportError:
        return None

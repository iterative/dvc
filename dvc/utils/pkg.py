from dvc.utils import is_binary


def check_build_patch():
    try:
        from .build import PKG  # file created during dvc build

        return PKG
    except ImportError:
        return None


def get_package_manager():
    if not is_binary():
        return "pip"

    return check_build_patch()

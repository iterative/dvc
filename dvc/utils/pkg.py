from dvc.utils import is_binary


def is_conda():
    try:
        from .build import PKG  # patched during conda package build

        return PKG == "conda"
    except ImportError:
        return False


def get_linux():
    import distro

    if not is_binary():
        return "pip"

    package_managers = {
        "rhel": "yum",
        "centos": "yum",
        "fedora": "yum",
        "amazon": "yum",
        "opensuse": "yum",
        "ubuntu": "apt",
        "debian": "apt",
    }

    return package_managers.get(distro.id())


def get_darwin():
    if not is_binary():
        if __file__.startswith("/usr/local/Cellar"):
            return "formula"
        else:
            return "pip"
    return None


def get_windows():
    return None if is_binary() else "pip"


def get_package_manager():
    import platform
    from dvc.exceptions import DvcException

    if is_conda():
        return "conda"

    m = {
        "Windows": get_windows(),
        "Darwin": get_darwin(),
        "Linux": get_linux(),
    }

    system = platform.system()
    func = m.get(system)
    if func is None:
        raise DvcException("not supported system '{}'".format(system))

    return func

import importlib.util
import os
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py

# Prevents pkg_resources import in entry point script,
# see https://github.com/ninjaaron/fast-entry_points.
# This saves about 200 ms on startup time for non-wheel installs.
try:
    import fastentrypoints  # noqa: F401, pylint: disable=unused-import
except ImportError:
    pass  # not able to import when installing through pre-commit


# Read package meta-data from version.py
# see https://packaging.python.org/guides/single-sourcing-package-version/
pkg_dir = os.path.dirname(os.path.abspath(__file__))
version_path = os.path.join(pkg_dir, "dvc", "version.py")
spec = importlib.util.spec_from_file_location("dvc.version", version_path)
dvc_version = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dvc_version)
version = dvc_version.__version__  # noqa: F821


# To achieve consistency between the build version and the one provided
# by your package during runtime, you need to **pin** the build version.
#
# This custom class will replace the version.py module with a **static**
# `__version__` that your package can read at runtime, assuring consistency.
#
# References:
#   - https://docs.python.org/3.7/distutils/extending.html
#   - https://github.com/python/mypy
class build_py(_build_py):
    def pin_version(self):
        path = os.path.join(self.build_lib, "dvc")
        self.mkpath(path)
        with open(os.path.join(path, "version.py"), "w") as fobj:
            fobj.write("# AUTOGENERATED at build time by setup.py\n")
            fobj.write(f'__version__ = "{version}"\n')

    def run(self):
        self.execute(self.pin_version, ())
        _build_py.run(self)


# Extra dependencies for remote integrations
requirements = {
    path.stem: path.read_text().strip().splitlines()
    for path in Path("requirements").glob("*.txt")
}

# gssapi should not be included in all_remotes, because it doesn't have wheels
# for linux and mac, so it will fail to compile if user doesn't have all the
# requirements, including kerberos itself. Once all the wheels are available,
# we can start shipping it by default.

install_requires = requirements.pop("default")
requirements["all"] = [
    requirements
    for key, requirements in requirements.items()
    if key not in ("tests", "ssh_gssapi", "terraform")
]
requirements["tests"] += requirements["terraform"]
requirements["dev"] = requirements["all"] + requirements["tests"]

setup(
    version=version,
    install_requires=install_requires,
    extras_require=requirements,
    cmdclass={"build_py": build_py},
)

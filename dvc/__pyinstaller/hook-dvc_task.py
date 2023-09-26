# ruff: noqa: N999
# pylint: disable=import-error
from PyInstaller.utils.hooks import collect_submodules

hiddenimports = collect_submodules("dvc_task")

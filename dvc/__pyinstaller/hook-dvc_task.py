# ruff: noqa: N999

from PyInstaller.utils.hooks import collect_submodules

hiddenimports = collect_submodules("dvc_task")

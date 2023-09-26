# ruff: noqa: N999
from PyInstaller.utils.hooks import copy_metadata  # pylint:disable=import-error

datas = copy_metadata("pydrive2")

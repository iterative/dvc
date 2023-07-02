# ruff: noqa: N999
import os

if os.name == "nt":
    hiddenimports = ["win32file"]

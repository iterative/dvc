# ruff: noqa: N999

from PyInstaller.utils.hooks import collect_submodules, is_module_or_submodule

# Celery dynamically imports most celery internals at runtime
# pyinstaller hook must expose all modules loaded by
# kombu.utils.imports:symbol_by_name()
_EXCLUDES = ("celery.bin", "celery.contrib")
hiddenimports = collect_submodules(
    "celery",
    filter=lambda name: not any(
        is_module_or_submodule(name, module) for module in _EXCLUDES
    ),
)

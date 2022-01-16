import glob
import os
import sys
from typing import List

import pluggy

from . import hookspecs
from .env import DVC_PLUGINS_DIR


def import_file(name, file):
    import importlib.util

    spec = importlib.util.spec_from_file_location(name, file)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class PluginManager(pluggy.PluginManager):
    def add_files_from_dir(self, directory: str) -> None:
        if not os.path.isdir(directory):
            return
        path = os.path.join(directory, "*.py")
        for file in filter(os.path.isfile, glob.glob(path)):
            mod = import_file(os.path.basename(file)[:-3], file)
            try:
                self.register(mod)
            except ValueError:
                pass

    def load_from_env(self, env: str = DVC_PLUGINS_DIR) -> None:
        if env in os.environ:
            self.add_files_from_dir(os.environ[env])

    def load_from_args(
        self, args: List[str] = None, opt_name: str = "--plugins"
    ) -> None:
        args = sys.argv[1:] if args is None else args
        i = 0
        n = len(args)
        while i < n:
            opt = args[i]
            i += 1
            if isinstance(opt, str):
                if opt == opt_name:
                    try:
                        parg = args[i]
                    except IndexError:
                        return
                    i += 1
                elif opt.startswith(f"{opt_name}="):
                    parg = opt[len(f"{opt_name}=") :]
                else:
                    continue
                self.add_files_from_dir(parg)


plugin_manager = PluginManager("dvc")
plugin_manager.add_hookspecs(hookspecs)

if "DVC_TEST" not in os.environ:  # we don't want to run this on tests
    plugin_manager.load_setuptools_entrypoints("dvc")
    plugin_manager.load_from_args()
    plugin_manager.load_from_env()

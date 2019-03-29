from __future__ import unicode_literals

import argparse

from dvc.command.base import fix_subparsers, append_doc_link
from dvc.command.remote import CmdRemoteAdd
from dvc.command.config import CmdConfig


class CmdCacheDir(CmdConfig):
    def run(self):
        self.args.name = "cache.dir"
        self.args.value = CmdRemoteAdd.resolve_path(
            self.args.value, self.configobj.filename
        )

        return super(CmdCacheDir, self).run()


def add_parser(subparsers, parent_parser):
    from dvc.command.config import parent_config_parser

    CACHE_HELP = "Manage cache settings."

    cache_parser = subparsers.add_parser(
        "cache",
        parents=[parent_parser],
        description=append_doc_link(CACHE_HELP, "cache"),
        help=CACHE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    cache_subparsers = cache_parser.add_subparsers(
        dest="cmd", help="Use dvc cache CMD --help for command-specific help."
    )

    fix_subparsers(cache_subparsers)

    parent_cache_config_parser = argparse.ArgumentParser(
        add_help=False, parents=[parent_config_parser]
    )
    CACHE_DIR_HELP = "Configure cache directory location."

    cache_dir_parser = cache_subparsers.add_parser(
        "dir",
        parents=[parent_cache_config_parser],
        description=append_doc_link(CACHE_HELP, "cache-dir"),
        help=CACHE_DIR_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    cache_dir_parser.add_argument(
        "-u",
        "--unset",
        default=False,
        action="store_true",
        help="Unset option.",
    )
    cache_dir_parser.add_argument(
        "value",
        default=None,
        help="Path to cache directory. Relative paths are resolved relative "
        "to the current directory and saved to config relative to the "
        "config file location.",
    )
    cache_dir_parser.set_defaults(func=CmdCacheDir)

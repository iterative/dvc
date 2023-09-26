import argparse
import os

from dvc.cli import completion
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link, fix_subparsers
from dvc.commands.config import CmdConfig
from dvc.ui import ui


class CmdCacheDir(CmdConfig):
    def run(self):
        if self.args.value is None and not self.args.unset:
            from dvc.config import ConfigError

            if self.args.level:
                conf = self.config.read(level=self.args.level)
            else:
                # Use merged config with default values
                conf = self.config
            try:
                self._check(conf, False, "cache", "dir")
                path = conf["cache"]["dir"]
            except ConfigError:
                if not self.config.dvc_dir or self.args.level:
                    raise
                path = os.path.join(self.config.dvc_dir, "cache")
            ui.write(path)
            return 0
        with self.config.edit(level=self.args.level) as conf:
            if self.args.unset:
                self._check(conf, False, "cache", "dir")
                del conf["cache"]["dir"]
            else:
                self._check(conf, False, "cache")
                conf["cache"]["dir"] = self.args.value
        return 0


class CmdCacheMigrate(CmdBase):
    def run(self):
        from dvc.cachemgr import migrate_2_to_3

        migrate_2_to_3(self.repo, dry=self.args.dry)
        return 0


def add_parser(subparsers, parent_parser):
    from dvc.commands.config import parent_config_parser

    CACHE_HELP = "Manage cache settings."

    cache_parser = subparsers.add_parser(
        "cache",
        parents=[parent_parser],
        description=append_doc_link(CACHE_HELP, "cache"),
        help=CACHE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    cache_subparsers = cache_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc cache CMD --help` for command-specific help.",
    )

    parent_cache_config_parser = argparse.ArgumentParser(
        add_help=False, parents=[parent_config_parser]
    )
    CACHE_DIR_HELP = "Configure cache directory location."

    cache_dir_parser = cache_subparsers.add_parser(
        "dir",
        parents=[parent_parser, parent_cache_config_parser],
        description=append_doc_link(CACHE_HELP, "cache/dir"),
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
        help=(
            "Path to cache directory. Relative paths are resolved relative "
            "to the current directory and saved to config relative to the "
            "config file location. If no path is provided, it returns the "
            "current cache directory."
        ),
        nargs="?",
    ).complete = completion.DIR
    cache_dir_parser.set_defaults(func=CmdCacheDir)

    CACHE_MIGRATE_HELP = "Migrate cached files to the DVC 3.0 cache location."
    cache_migrate_parser = cache_subparsers.add_parser(
        "migrate",
        parents=[parent_parser],
        description=append_doc_link(CACHE_HELP, "cache/migrate"),
        help=CACHE_MIGRATE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    cache_migrate_parser.add_argument(
        "--dry",
        help=(
            "Only print actions which would be taken without actually migrating "
            "any data."
        ),
        action="store_true",
    )
    cache_migrate_parser.set_defaults(func=CmdCacheMigrate)

    fix_subparsers(cache_subparsers)

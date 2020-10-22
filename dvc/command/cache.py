import argparse
import json
import logging
import os

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.command.config import CmdConfig
from dvc.dvcfile import Dvcfile
from dvc.utils.diff import table
from dvc.utils.fs import walk_files

logger = logging.getLogger(__name__)


class CmdCacheDir(CmdConfig):
    def run(self):
        if self.args.value is None and not self.args.unset:
            logger.info(self.config["cache"]["dir"])
            return 0
        with self.config.edit(level=self.args.level) as edit:
            edit["cache"]["dir"] = self.args.value
        return 0


class CmdCacheStatus(CmdBase):
    def run(self):
        try:
            self.permission_check()
        except Exception as e:
            logger.error(e)
            return 1
        try:
            self.show_dvc_files_status()
        except Exception as e:
            logger.error(e)
            return 1
        return 0

    def permission_check(self):
        cache_dir = self.config["cache"]["dir"]
        logger.info("Step 1: Permission Check on: {}".format(cache_dir))
        logger.info(
            "\tRead: {}".format(
                "OK" if os.access(cache_dir, os.R_OK) else "Not OK"
            )
        )
        logger.info(
            "\tWrite: {}".format(
                "OK" if os.access(cache_dir, os.W_OK) else "Not OK"
            )
        )
        logger.info(
            "\tExist: {}".format(
                "OK" if os.access(cache_dir, os.F_OK) else "Not OK"
            )
        )

    def show_dvc_files_status(self):
        logger.info("Step 2: DVC Files and Cache status.")

        cache_dir = self.config["cache"]["dir"]
        root_dir = self.repo.root_dir + os.path.sep

        rows = []
        cached_files, dvc_files = self._get_dvc_and_cached_files()

        for cached_file in cached_files:
            md5_stage_path = "".join(
                cached_file.replace(cache_dir, "").split("/")
            )
            filename = dvc_files[md5_stage_path]
            row = [
                "Dir" if cached_file.endswith(".dir") else "File",
                filename.replace(root_dir, ""),
                "-"
                if cached_file.endswith(".dir")
                else os.stat(filename).st_size,
                cached_file.replace(root_dir, ""),
                "-"
                if cached_file.endswith(".dir")
                else os.stat(cached_file).st_size,
            ]
            rows.append(row)

        header = ["File/Directory", "Name", "Size", "CachePath", "CacheSize"]
        logger.info(table(header, rows, markdown=False))

    def _get_dvc_and_cached_files(self):
        cache_dir = self.config["cache"]["dir"]
        cached_files = list(walk_files(cache_dir))
        cached_dirs = [file for file in cached_files if file.endswith(".dir")]
        dvc_files = {}

        for path in self.repo.tree.list_paths():
            if path.endswith(".dvc"):
                dvcfile = Dvcfile(self.repo, path)
                for stage_file in dvcfile.stages.stage_data["outs"]:
                    dvc_files[stage_file["md5"]] = "{}/{}".format(
                        os.path.dirname(path), stage_file["path"]
                    )

        for file in cached_dirs:
            md5_stage_path = "".join(file.replace(cache_dir, "").split("/"))
            with open(file, "r") as handler:
                for stage_file in json.loads(handler.read()):
                    dvc_files[stage_file["md5"]] = "{}/{}".format(
                        dvc_files[md5_stage_path], stage_file["relpath"]
                    )

        return cached_files, dvc_files


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
        dest="cmd",
        help="Use `dvc cache CMD --help` for command-specific " "help.",
    )

    fix_subparsers(cache_subparsers)

    parent_cache_config_parser = argparse.ArgumentParser(
        add_help=False, parents=[parent_config_parser]
    )
    CACHE_DIR_HELP = "Configure cache directory location."
    CACHE_CHECK_HELP = "Check and show cache status"

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
        help="Path to cache directory. Relative paths are resolved relative "
        "to the current directory and saved to config relative to the "
        "config file location. If no path is provided, it returns the "
        "current cache directory.",
        nargs="?",
    ).complete = completion.DIR
    cache_dir_parser.set_defaults(func=CmdCacheDir)

    cache_status_parser = cache_subparsers.add_parser(
        "status",
        parents=[parent_parser, parent_cache_config_parser],
        description=append_doc_link(CACHE_HELP, "cache/status"),
        help=CACHE_CHECK_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    cache_status_parser.set_defaults(func=CmdCacheStatus)

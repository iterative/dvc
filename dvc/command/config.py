from __future__ import unicode_literals

import os
import argparse
import logging

from dvc.command.base import CmdBaseNoRepo, append_doc_link
from dvc.config import Config
from dvc.exceptions import DvcException


logger = logging.getLogger(__name__)


class CmdConfig(CmdBaseNoRepo):
    def __init__(self, args):
        from dvc.repo import Repo, NotDvcRepoError

        super(CmdConfig, self).__init__(args)

        try:
            dvc_dir = os.path.join(Repo.find_root(), Repo.DVC_DIR)
            saved_exc = None
        except NotDvcRepoError as exc:
            dvc_dir = None
            saved_exc = exc

        self.config = Config(dvc_dir, validate=False)
        if self.args.system:
            self.configobj = self.config._system_config
        elif self.args.glob:
            self.configobj = self.config._global_config
        elif self.args.local:
            if dvc_dir is None:
                raise saved_exc
            self.configobj = self.config._local_config
        else:
            if dvc_dir is None:
                raise saved_exc
            self.configobj = self.config._repo_config

    def _unset(self, section, opt=None, configobj=None):
        if configobj is None:
            configobj = self.configobj

        try:
            self.config.unset(configobj, section, opt)
            self.config.save(configobj)
        except DvcException:
            logger.exception("failed to unset '{}'".format(self.args.name))
            return 1
        return 0

    def _show(self, section, opt):
        try:
            self.config.show(self.configobj, section, opt)
        except DvcException:
            logger.exception("failed to show '{}'".format(self.args.name))
            return 1
        return 0

    def _set(self, section, opt, value):
        try:
            self.config.set(self.configobj, section, opt, value)
            self.config.save(self.configobj)
        except DvcException:
            logger.exception(
                "failed to set '{}.{}' to '{}'".format(section, opt, value)
            )
            return 1
        return 0

    def run(self):
        section, opt = self.args.name.lower().strip().split(".", 1)

        if self.args.unset:
            return self._unset(section, opt)
        elif self.args.value is None:
            return self._show(section, opt)
        else:
            return self._set(section, opt, self.args.value)


parent_config_parser = argparse.ArgumentParser(add_help=False)
parent_config_parser.add_argument(
    "--global",
    dest="glob",
    action="store_true",
    default=False,
    help="Use global config.",
)
parent_config_parser.add_argument(
    "--system", action="store_true", default=False, help="Use system config."
)
parent_config_parser.add_argument(
    "--local", action="store_true", default=False, help="Use local config."
)


def add_parser(subparsers, parent_parser):
    CONFIG_HELP = "Get or set config options."

    config_parser = subparsers.add_parser(
        "config",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(CONFIG_HELP, "config"),
        help=CONFIG_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    config_parser.add_argument(
        "-u",
        "--unset",
        default=False,
        action="store_true",
        help="Unset option.",
    )
    config_parser.add_argument("name", help="Option name.")
    config_parser.add_argument(
        "value", nargs="?", default=None, help="Option value."
    )
    config_parser.set_defaults(func=CmdConfig)

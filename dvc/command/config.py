import os

from dvc.command.base import CmdBase
from dvc.logger import Logger
from dvc.config import Config
from dvc.exceptions import DvcException


class CmdConfig(CmdBase):
    def __init__(self, args):
        from dvc.project import Project, NotDvcProjectError

        self.args = args

        try:
            dvc_dir = os.path.join(Project._find_root(), Project.DVC_DIR)
            saved_exc = None
        except NotDvcProjectError as exc:
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
            self.configobj = self.config._project_config

    def run_cmd(self):
        return self.run()

    def _unset(self, section, opt=None, configobj=None):
        if configobj is None:
            configobj = self.configobj

        try:
            self.config.unset(configobj, section, opt)
            self.config.save(configobj)
        except DvcException as exc:
            Logger.error("Failed to unset '{}'".format(self.args.name), exc)
            return 1
        return 0

    def _show(self, section, opt):
        try:
            self.config.show(self.configobj, section, opt)
        except DvcException as exc:
            Logger.error("Failed to show '{}'".format(self.args.name), exc)
            return 1
        return 0

    def _set(self, section, opt, value):
        try:
            self.config.set(self.configobj, section, opt, value)
            self.config.save(self.configobj)
        except DvcException as exc:
            Logger.error("Failed to set '{}.{}' to '{}'".format(section,
                                                                opt,
                                                                value),
                         exc)
            return 1
        return 0

    def run(self):
        section, opt = self.args.name.lower().strip().split('.', 1)

        if self.args.unset:
            return self._unset(section, opt)
        elif self.args.value is None:
            return self._show(section, opt)
        else:
            return self._set(section, opt, self.args.value)

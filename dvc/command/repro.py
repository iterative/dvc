import os
import copy

from dvc.command.base import DvcLock
from dvc.command.run import CmdRun, CommandFile
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.path.data_item import DataDirError
from dvc.state_file import StateFile
from dvc.system import System
from dvc.data_cloud import file_md5


class ReproError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Run error: {}'.format(msg))


class CmdRepro(CmdRun):
    def __init__(self, settings):
        super(CmdRepro, self).__init__(settings)

    def run(self):
        recursive = not self.parsed_args.single_item
        targets = []

        if self.parsed_args.target:
            targets += self.parsed_args.target
        else:
            target = self.settings.config._config['Global'].get('Target', None)
            if not target or len(target) == 0:
                Logger.error('Reproduction target is not defined. ' +
                             'Specify data file or set target by ' +
                             '`dvc config global.target target` command.')
                return 1
            targets += [target]

        return self.repro_target(targets, recursive, self.parsed_args.force)

    def repro_target(self, target, recursive, force):
        if not self.no_git_actions and not self.git.is_ready_to_go():
            return 1

        data_item_list, external_files_names = self.settings.path_factory.to_data_items(target)
        if external_files_names:
            Logger.error('Files from outside of the repo could not be reproduced: {}'.
                         format(' '.join(external_files_names)))
            return 1

        if self.repro_data_items(data_item_list, recursive, force):
            return 0
        return 1

    def repro_data_items(self, data_item_list, recursive, force):
        error = False
        changed = False

        for data_item in data_item_list:
            try:
                change = ReproChange(data_item, self, recursive, force)
                if change.reproduce():
                    changed = True
                    Logger.info(u'Data item "{}" was reproduced.'.format(
                        data_item.data.relative
                    ))
                else:
                    Logger.info(u'Reproduction is not required for data item "{}".'.format(
                        data_item.data.relative
                    ))
            except ReproError as err:
                Logger.error('Error in reproducing data item {}: {}'.format(
                    data_item.data.relative, str(err)
                ))
                error = True
                break

        if error and not self.no_git_actions:
            Logger.error('Errors occurred. One or more repro cmd was not successful.')
            self.not_committed_changes_warning()

        return changed and not error


class ReproChange(object):
    def __init__(self, data_item, cmd_obj, recursive, force):
        self._data_item = data_item
        self.git = cmd_obj.git
        self._cmd_obj = cmd_obj
        self.settings = cmd_obj.settings
        self._recursive = recursive
        self._force = force

        try:
            self.state = StateFile.load(data_item, self.settings)
        except Exception as ex:
            raise ReproError('Error: state file "{}" cannot be loaded: {}'.
                             format(data_item.state.relative, ex))

        if isinstance(self.state.command, str):
            self.command = CommandFile.load(self.state.command)
        else:
            self.command = CommandFile.loadd(self.state.command)

        if not self.command.cmd and not self.command.locked:
            raise ReproError('Error: parameter {} is not defined in "{}"'.
                            format(CommandFile.PARAM_CMD, self.command.fname if self.command.fname else data_item.state.relative))

        self._settings = copy.copy(self._cmd_obj.settings)

    def is_cache_exists(self):
        path = System.realpath(self._data_item.data.relative)
        return os.path.exists(path)

    @property
    def cmd_obj(self):
        return self._cmd_obj

    def remove_output_files(self):
        for output_dvc in self.command.out:
            Logger.debug('Removing output file {} before reproduction.'.format(output_dvc))

            try:
                data_item = self.cmd_obj.settings.path_factory.data_item_from_dvc_path(output_dvc)
                os.remove(data_item.data.relative)
            except Exception as ex:
                msg = 'Data item {} cannot be removed before reproduction: {}'
                Logger.error(msg.format(output_dvc, ex))

    def reproduce_run(self):
        Logger.info('Reproducing run command for data item {}. Args: {}'.format(
            self._data_item.data.relative, self.command.cmd))

        CmdRun.run_command(self.settings, self.command)
        self._cmd_obj.commit_if_needed('DVC repro: {}'.format(self.command.cmd)) 

    def reproduce_data_item(self):
        Logger.debug('Reproducing data item {}.'.format(self._data_item.data.dvc))
        self.remove_output_files()
        self.reproduce_run()

    def reproduce(self):
        Logger.debug('Reproduce data item {}. recursive={}, force={}'.format(
            self._data_item.data.relative, self._recursive, self._force))

        if self.command.locked:
            Logger.debug('Data item {} is not reproducible'.format(self._data_item.data.relative))
            return False

        repro_required = False
        deps_changed = self.reproduce_deps(self._data_item, self._recursive)
        if deps_changed or self._force or not self.is_cache_exists():
            repro_required = True

        if not repro_required:
            Logger.debug('Data item {} is up to date'.format(self._data_item.data.relative))
            return False

        Logger.debug('Data item {} is going to be reproduced'.format(self._data_item.data.relative))
        self.reproduce_data_item()
        return True

    def reproduce_deps(self, data_item_dvc, recursive):
        result = False

        for dep in self.state.deps:
            path = dep[StateFile.PARAM_PATH]
            md5 = dep[StateFile.PARAM_MD5]

            if not self._settings.path_factory.is_data_item(path):
                if md5 != file_md5(os.path.join(self._settings.git.git_dir_abs, path))[0]:
                    self.log_repro_reason('source {} was changed'.format(path))
                    result = True
                continue

            item = self._settings.path_factory.existing_data_item(path)
            if recursive:
                change = ReproChange(item, self._cmd_obj, self._recursive, self._force)
                if change.reproduce():
                   result = True

            if md5 != os.path.basename(item.cache.relative):
                self.log_repro_reason('data item {} was changed'.format(path))
                result = True

        return result

    def log_repro_reason(self, reason):
        msg = u'Repro is required for data item {} because of {}'
        Logger.debug(msg.format(self._data_item.data.relative, reason))

import os
import copy

from dvc.command.import_file import CmdImportFile
from dvc.command.run import CmdRun
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

        self._code = []
        pass

    @property
    def code_dependencies(self):
        return self._code

    @property
    def lock(self):
        return True

    @property
    def declaration_input_data_items(self):
        return []

    @property
    def declaration_output_data_items(self):
        return []

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
            self._state = StateFile.load(data_item, self.settings)
        except Exception as ex:
            raise ReproError('Error: state file "{}" cannot be loaded: {}'.
                             format(data_item.state.relative, ex))

        if not self.state.argv:
            raise ReproError('Error: parameter {} is not defined in state file "{}"'.
                             format(StateFile.PARAM_ARGV, data_item.state.relative))
        if len(self.state.argv) < 1:
            raise ReproError('Error: reproducible cmd in state file "{}" is too short'.
                             format(self.state.file))

        self._settings = copy.copy(self._cmd_obj.settings)
        self._settings.set_args(self.state.argv)
        pass

    def is_cache_exists(self):
        path = System.realpath(self._data_item.data.relative)
        return os.path.exists(path)

    @property
    def cmd_obj(self):
        return self._cmd_obj

    @property
    def state(self):
        return self._state

    def remove_output_files(self):
        for output_dvc in self._state.output_files:
            Logger.debug('Removing output file {} before reproduction.'.format(output_dvc))

            try:
                data_item = self.cmd_obj.settings.path_factory.data_item_from_dvc_path(output_dvc)
                os.remove(data_item.data.relative)
            except Exception as ex:
                msg = 'Data item {} cannot be removed before reproduction: {}'
                Logger.error(msg.format(output_dvc, ex))

    def reproduce_import(self):
        Logger.debug('Reproducing data item {}. Re-import cmd: {}'.format(
            self._data_item.data.relative, ' '.join(self.state.argv)))

        if len(self.state.argv) != 2:
            msg = 'Data item "{}" cannot be re-imported because of arguments number {} is incorrect. Argv: {}'
            raise ReproError(msg.format(self._data_item.data.relative, len(self.state.argv), self.state.argv))

        input = self.state.argv[0]
        output = self.state.argv[1]

        cmd = CmdImportFile(self._settings)
        cmd.set_git_action(True)
        cmd.set_locker(False)

        Logger.info(u'Reproducing import command: {}'.format(output))
        if cmd.import_and_commit_if_needed(input, output, lock=True,
                                           check_if_ready=False,
                                           is_repro=True) != 0:
            raise ReproError('Import command reproduction failed')

    def reproduce_run(self):
        cmd = CmdRun(self._settings)
        cmd.set_git_action(True)
        cmd.set_locker(False)

        Logger.info('Reproducing run command for data item {}. Args: {}'.format(
            self._data_item.data.relative, ' '.join(self.state.argv)))

        data_items_from_args, not_data_items_from_args = self.cmd_obj.argv_files_by_type(self.state.argv)
        if cmd.run_and_commit_if_needed(self.state.argv,
                                        data_items_from_args,
                                        not_data_items_from_args,
                                        self.state.stdout,
                                        self.state.stderr,
                                        self.state.shell,
                                        [],
                                        [],
                                        [],
                                        False,
                                        check_if_ready=False,
                                        is_repro=True) != 0:
            raise ReproError('Run command reproduction failed')

    def reproduce_data_item(self):
        Logger.debug('Reproducing data item {}.'.format(self._data_item.data.dvc))

        self.remove_output_files()

        if self.state.is_import_file:
            self.reproduce_import()
        elif self.state.is_run:
            self.reproduce_run()
        # Ignore EMPTY_FILE command

    def reproduce(self):
        Logger.debug('Reproduce data item {}. recursive={}, force={}'.format(
            self._data_item.data.relative, self._recursive, self._force))

        if self.state.locked:
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

        for dep in self._state.deps:
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

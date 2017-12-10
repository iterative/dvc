import os
import copy

from dvc.command.run import CmdRun
from dvc.logger import Logger
from dvc.exceptions import DvcException
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
        stages = []

        for target in self.parsed_args.targets:
            if StateFile._is_state_file(target):
                stage = StateFile.load(target)
            else:
                stage = StateFile.find_by_output(self.settings, target)

            if stage:
                stages.append(stage)

        self.repro_stages(stages, recursive, self.parsed_args.force)
        names = [os.path.relpath(stage.path) for stage in stages]
        return self.commit_if_needed('DVC repro: {}'.format(names))

    def repro_stages(self, stages, recursive, force):
        error = False
        changed = False

        for stage in stages:
            try:
                change = ReproStage(self.settings, stage, recursive, force)
                if change.reproduce():
                    changed = True
                    Logger.info(u'Stage "{}" was reproduced.'.format(stage.path))
                else:
                    Logger.info(u'Reproduction is not required for stage "{}".'.format(stage.path))
            except ReproError as err:
                Logger.error('Error in reproducing stage {}: {}'.format(stage.path, str(err)))
                error = True
                break

        if error and not self.no_git_actions:
            Logger.error('Errors occurred. One or more repro cmd was not successful.')
            self.not_committed_changes_warning()

        return changed and not error


class ReproStage(object):
    def __init__(self, settings, stage, recursive, force):
        self.git = settings.git
        self.settings = settings
        self._recursive = recursive
        self._force = force

        self.stage = stage

        if not self.stage.cmd and not self.stage.locked:
            msg = 'Error: stage "{}" is not locked, but has no command for reproduction'
            raise ReproError(msg.format(stage.path))

    def is_cache_exists(self):
        for out in self.stage.out:
            path = os.path.join(self.stage.cwd, out)
            if not os.path.exists(path):
                return False
        return True

    def remove_output_files(self):
        for out in self.stage.out:
            path = os.path.join(self.stage.cwd, out)
            Logger.debug('Removing output file {} before reproduction.'.format(path))
            try:
                os.remove(path)
            except Exception as ex:
                msg = 'Output file {} cannot be removed before reproduction: {}'
                Logger.debug(msg.format(path, ex))

    def reproduce_run(self):
        Logger.info('Reproducing run command for stage {}. Args: {}'.format(
            self.stage.path, self.stage.cmd))

        CmdRun.run_command(self.settings, self.stage)

    def reproduce_stage(self):
        Logger.debug('Reproducing stage {}.'.format(self.stage.path))
        self.remove_output_files()
        self.reproduce_run()

    def is_repro_required(self):
        deps_changed = self.reproduce_deps()
        if deps_changed or self._force or not self.is_cache_exists():
            return True
        return False

    def reproduce(self):
        Logger.debug('Reproduce stage {}. recursive={}, force={}'.format(
            self.stage.path, self._recursive, self._force))

        if self.stage.locked:
            Logger.debug('Stage {} is not reproducible'.format(self.stage.path))
            return False

        if not self.is_repro_required():
            Logger.debug('Stage {} is up to date'.format(self.stage.path))
            return False

        Logger.debug('Stage {} is going to be reproduced'.format(self.stage.path))
        self.reproduce_stage()
        return True

    def reproduce_dep(self, path, md5, recursive):
        if not self.settings.path_factory.is_data_item(path):
            if md5 != file_md5(os.path.join(self.git.git_dir_abs, path))[0]:
                self.log_repro_reason('source {} was changed'.format(path))
                return True
            return False

        stage = StateFile.find_by_output(self.settings, path)
        if recursive:
            ReproStage(self.settings, stage, self._recursive, self._force).reproduce()

        stage = StateFile.load(stage.path)
        if md5 != stage.out[os.path.relpath(path, stage.cwd)]:
            self.log_repro_reason('data item {} was changed - md5 sum doesn\'t match'.format(path))
            return True

        return False

    def reproduce_deps(self):
        result = False

        for name,md5 in self.stage.deps.items():
            path = os.path.join(self.stage.cwd, name)
            if self.reproduce_dep(path, md5, self._recursive):
                result = True

        return result

    def log_repro_reason(self, reason):
        msg = u'Repro is required for stage {} because of {}'
        Logger.debug(msg.format(self.stage.path, reason))

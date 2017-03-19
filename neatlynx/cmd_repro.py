import os

import fasteners

from neatlynx.cmd_base import CmdBase
from neatlynx.cmd_run import CmdRun
from neatlynx.git_wrapper import GitWrapper
from neatlynx.logger import Logger
from neatlynx.exceptions import NeatLynxException
from neatlynx.data_file_obj import DataFileObj, NotInDataDirError
from neatlynx.state_file import StateFile


class ReproError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Run error: {}'.format(msg))


class CmdRepro(CmdRun):
    def __init__(self):
        CmdBase.__init__(self)

        self._code =[]
        pass

    @property
    def code(self):
        return self._code

    def define_args(self, parser):
        self.set_skip_git_actions(parser)

        # self.add_string_arg(parser, 'target', 'Reproduce data file')
        parser.add_argument('target', metavar='', help='Data file to reproduce', nargs='*')
        pass

    # Overridden methods:

    @property
    def declaration_input_files(self):
        return []

    @property
    def declaration_output_files(self):
        return []

    def run(self):
        lock = fasteners.InterProcessLock(self.git.lock_file)
        gotten = lock.acquire(timeout=5)
        if not gotten:
            Logger.printing('Cannot perform the command since NLX is busy and locked. Please retry the command later.')
            return 1

        try:
            if not self.skip_git_actions and not self.git.is_ready_to_go():
                return 1

            dobjs, externally_created_files = DataFileObj.files_to_dobjs(self.args.target, self.git, self.config)
            if externally_created_files:
                Logger.error('Files from outside of the data directory "{}" could not be reproduced: {}'.
                             format(self.config.data_dir, ' '.join(externally_created_files)))
                return 1

            error = False
            changed = False
            for dobj in dobjs:
                try:
                    repro_change = ReproChange(dobj, self.git, self)
                    if repro_change.reproduce():
                        changed = True
                        Logger.info(u'Data file "{}" was reproduced.'.format(
                            dobj.data_file_relative
                        ))
                    else:
                        Logger.info(u'Reproduction is not required for data file "{}".'.format(
                            dobj.data_file_relative
                        ))
                except ReproError as err:
                    Logger.error('Error in reproducing data file {}: {}'.format(
                        dobj.data_file_relative, str(err)
                    ))
                    error = True
                    break

            if error and not self.skip_git_actions:
                Logger.error('Errors occurred. One or more repro command was not successful.')
                self.not_committed_changes_warning()
                return 1

            if changed:
                if self.skip_git_actions:
                    self.not_committed_changes_warning()
                    return 1

                message = 'NLX repro: {}'.format(' '.join(self.args.target))
                self.git.commit_all_changes_and_log_status(message)
        finally:
            lock.release()
        return 0


class ReproChange(object):
    def __init__(self, dobj, git, cmd_obj):
        self._dobj = dobj
        self._state = StateFile.load(dobj.state_file_relative, git)
        self.git = git
        self._cmd_obj = cmd_obj

        cmd_obj._code = self._state.code_sources # HACK!!!

        argv = self._state.norm_argv

        if not argv:
            raise ReproError('Error: parameter {} is nor defined in state file "{}"'.
                             format(StateFile.PARAM_NORM_ARGV, dobj.state_file_relative))
        if len(argv) < 2:
            raise ReproError('Error: reproducible command in state file "{}" is too short'.
                             format(self._state.file))

        # if argv[0][-3:] != '.py':
        #     raise ReproError('Error: reproducible command format error in state file "{}"'.
        #                      format(self._state.file))

        self._repro_argv = argv
        pass

    def were_direct_dependencies_changed(self):
        return True

    def reproduce_data_file(self):
        Logger.debug('Reproducing data file "{}". Removing the file...'.format(
            self._dobj.data_file_relative))
        os.remove(self._dobj.data_file_relative)

        Logger.debug('Reproducing data file "{}". Re-runs command: {}'.format(
            self._dobj.data_file_relative, ' '.join(self._repro_argv)))
        return self._cmd_obj.run_command(self._repro_argv)

    def reproduce(self, force=False):
        were_input_files_changed = False

        if not self._state.is_reproducible:
            Logger.debug('Data file "{}" is not reproducible'.format(self._dobj.data_file_relative))
            return False

        for input_file in self._state.input_files:
            try:
                dobj = DataFileObj(input_file, self.git, self._cmd_obj.config)
            except NotInDataDirError:
                raise ReproError(u'The dependency files "{}" is not a data file'.format(input_file))
            except Exception as ex:
                raise ReproError(u'The dependency files "{}" can not be reproduced: {}'.format(
                                 input_file, ex))

            change = ReproChange(dobj, self.git, self._cmd_obj)
            if change.reproduce(force):
                were_input_files_changed = True

        was_source_code_changed = self.git.were_files_changed(self._dobj.data_file_relative,
                                                              self._state.code_sources)

        if not force and not was_source_code_changed and not were_input_files_changed:
            Logger.debug('Data file "{}" is up to date'.format(
                self._dobj.data_file_relative))
            return False

        return self.reproduce_data_file()

if __name__ == '__main__':
    import sys
    try:
        sys.exit(CmdRepro().run())
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)

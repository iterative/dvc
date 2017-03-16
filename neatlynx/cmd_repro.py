import os

import fasteners

from neatlynx.cmd_base import CmdBase
from neatlynx.cmd_run import CmdRun
from neatlynx.git_wrapper import GitWrapper
from neatlynx.logger import Logger
from neatlynx.exceptions import NeatLynxException
from neatlynx.data_file_obj import DataFileObj
from neatlynx.state_file import StateFile


class ReproError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Run error: {}'.format(msg))


class CmdRepro(CmdRun):
    def __init__(self):
        CmdBase.__init__(self)
        pass

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
            for dobj in dobjs:
                self.repro(dobj)

                # if returncode != 0:
                #     error = True
                #     Logger.error('Error: cannot reproduce file "{}"\n{}'.format(
                #         dobj.data_file_relative, out))
                #     sys.stderr.write('{}\n'.format(err))

            if error and not self.skip_git_actions:
                Logger.error('Errors occurred. One or more repro command was not successful.')
                self.not_committed_changes_warning()
                return 1

            if self.skip_git_actions:
                self.not_committed_changes_warning()
                return 0

            message = 'NLX repro: {}'.format(' '.join(self.args.target))
            self.git.commit_all_changes_and_log_status(message)
        finally:
            lock.release()

        # dobj = DataFileObj(self.args.target, self.git, self.config)
        # os.remove(self.args.target)
        #
        # state_file = StateFile(dobj.state_file_relative, self.git)
        # returncode, out, err = state_file.repro()
        #
        # print(out)
        # sys.stderr.write(err)
        #
        # return returncode
        return 0

    def repro(self, dobj):
        state = StateFile.load(dobj.state_file_relative, self.git)

        argv = state.norm_argv

        if not argv:
            raise ReproError('Error: parameter {} is nor defined in state file "{}"'.
                             format(StateFile.PARAM_NORM_ARGV, dobj.state_file_relative))
        if len(argv) < 2:
            raise ReproError('Error: reproducible command in state file "{}" is too short'.
                             format(self.file))

        if argv[0][-3:] != '.py':
            raise ReproError('Error: reproducible command format error in state file "{}"'.
                             format(self.file))
        argv.pop(0)

        Logger.debug('Removing data file "{}"'.format(dobj.data_file_relative))
        os.remove(dobj.data_file_relative)

        Logger.debug("Repro cmd:\n\t{}".format(' '.join(argv)))
        return self.run_command(argv)


if __name__ == '__main__':
    import sys
    try:
        sys.exit(CmdRepro().run())
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)

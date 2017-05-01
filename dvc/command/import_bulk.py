import os
import fasteners

from dvc.command.base import CmdBase
from dvc.command.import_file import CmdImportFile
from dvc.logger import Logger
from dvc.runtime import Runtime


class CmdImportBulk(CmdBase):
    def __init__(self, settings):
        super(CmdImportBulk, self).__init__(settings)

    def define_args(self, parser):
        self.set_no_git_actions(parser)
        self.set_lock_action(parser)

        parser.add_argument('input',
                            metavar='',
                            help='Input files.',
                            nargs='*')

        self.add_string_arg(parser, 'output', 'Output file.')

        pass

    def run(self):
        if self.is_locker:
            lock = fasteners.InterProcessLock(self.git.lock_file)
            gotten = lock.acquire(timeout=5)
            if not gotten:
                Logger.info('Cannot perform the cmd since DVC is busy and locked. Please retry the cmd later.')
                return 1

        try:
            if not self.no_git_actions and not self.git.is_ready_to_go():
                return 1

            cmd = CmdImportFile(self.settings)
            cmd.set_git_action(not self.no_git_actions)
            cmd.set_locker(False)

            output = self.parsed_args.output
            for input in self.parsed_args.input:
                if not os.path.isdir(input):
                    cmd.import_and_commit_if_needed(input, output, self.parsed_args.lock)
                else:
                    input_dir = os.path.basename(input)
                    for root, dirs, files in os.walk(input):
                        for file in files:
                            filename = os.path.join(root, file)

                            rel = os.path.relpath(filename, input)
                            out = os.path.join(output, input_dir, rel)

                            out_dir = os.path.dirname(out)
                            if not os.path.exists(out_dir):
                                os.mkdir(out_dir)

                            cmd.import_and_commit_if_needed(filename, out, self.parsed_args.lock)
                pass
        finally:
            if self.is_locker:
                lock.release()
        pass

if __name__ == '__main__':
    Runtime.run(CmdImportBulk)

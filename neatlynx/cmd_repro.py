import os

from neatlynx.cmd_base import CmdBase
from neatlynx.logger import Logger
from neatlynx.exceptions import NeatLynxException
from neatlynx.data_file_obj import DataFileObj
from neatlynx.state_file import StateFile


class ReproError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Run error: {}'.format(msg))


class CmdRepro(CmdBase):
    def __init__(self):
        CmdBase.__init__(self)
        pass

    def define_args(self, parser):
        self.add_string_arg(parser, 'target', 'Reproduce data file')
        pass

    def run(self):
        if not self.git.is_ready_to_go():
            return 1

        dobj = DataFileObj(self.args.target, self.git, self.config)
        os.remove(self.args.target)

        state_file = StateFile(dobj.state_file_relative, self.git)
        returncode, out, err = state_file.repro()

        print(out)
        sys.stderr.write(err)

        return returncode


if __name__ == '__main__':
    import sys
    try:
        sys.exit(CmdRepro().run())
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)

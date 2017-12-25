import os

from dvc.command.common.base import CmdBase
from dvc.logger import Logger
from dvc.stage import Stage


class CmdRun(CmdBase):
    def run(self):
        fname = self.get_stage_file(self.args.file, self.args.outs, self.args.outs_no_cache)

        self.project.run(cmd=' '.join(self.args.command),
                         outs=self.args.outs,
                         outs_no_cache=self.args.outs_no_cache,
                         deps=self.args.deps,
                         deps_no_cache=self.args.deps_no_cache,
                         locked=self.args.lock,
                         fname=fname,
                         cwd=self.args.cwd)
        return 0

    @staticmethod
    def get_stage_file(args_file, args_out, args_outs_no_cache):
        if args_file:
            return args_file

        if args_out or args_outs_no_cache:
            result = args_out[0] if args_out else args_outs_no_cache[0]
            result = os.path.basename(result)
            result += Stage.STAGE_FILE_SUFFIX
        else:
            result = Stage.STAGE_FILE

        Logger.info(u'Using \'{}\' as a stage file'.format(result))
        return result

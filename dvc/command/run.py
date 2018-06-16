import os

from dvc.command.common.base import CmdBase
from dvc.logger import Logger
from dvc.stage import Stage
from dvc.exceptions import DvcException


class CmdRun(CmdBase):
    def run(self):
        fname = self.stage_file_name(self.args.file,
                                     self.args.outs,
                                     self.args.outs_no_cache,
                                     self.args.metrics_no_cache)

        try:
            self.project.run(cmd=' '.join(self.args.command),
                             outs=self.args.outs,
                             outs_no_cache=self.args.outs_no_cache,
                             metrics_no_cache=self.args.metrics_no_cache,
                             deps=self.args.deps,
                             fname=fname,
                             cwd=self.args.cwd,
                             no_exec=self.args.no_exec)
        except DvcException as ex:
            self.project.logger.error('Failed to run command', ex)
            return 1

        return 0

    @staticmethod
    def stage_file_name(args_file, args_outs, args_outs_no_cache, args_metrics_no_cache):
        if args_file:
            return args_file

        if args_outs:
            result = CmdRun._stage_file_basename(args_outs[0])
        elif args_outs_no_cache:
            result = CmdRun._stage_file_basename(args_outs_no_cache[0])
        elif args_metrics_no_cache:
            result = CmdRun._stage_file_basename(args_metrics_no_cache[0])
        else:
            result = Stage.STAGE_FILE

        Logger.info(u'Using \'{}\' as a stage file'.format(result))
        return result

    @staticmethod
    def _stage_file_basename(fname):
        result = os.path.basename(fname)
        if len(result) == 0:
            result = os.path.basename(os.path.dirname(fname))
        result += Stage.STAGE_FILE_SUFFIX
        return result

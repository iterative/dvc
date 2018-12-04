from dvc.command.base import CmdBase
from dvc.exceptions import DvcException


class CmdRun(CmdBase):
    def _joined_cmd(self):
        if len(self.args.command) == 0:
            return ''

        if len(self.args.command) == 1:
            return self.args.command[0]

        cmd = ''
        for chunk in self.args.command:
            if len(chunk.split()) != 1:
                fmt = ' "{}"'
            else:
                fmt = ' {}'
            cmd += fmt.format(chunk)
        return cmd

    def run(self):
        overwrite = (self.args.yes or self.args.overwrite_dvcfile)
        try:
            self.project.run(cmd=self._joined_cmd(),
                             outs=self.args.outs,
                             outs_no_cache=self.args.outs_no_cache,
                             metrics_no_cache=self.args.metrics_no_cache,
                             deps=self.args.deps,
                             fname=self.args.file,
                             cwd=self.args.cwd,
                             no_exec=self.args.no_exec,
                             overwrite=overwrite,
                             ignore_build_cache=self.args.ignore_build_cache,
                             remove_outs=self.args.remove_outs)
        except DvcException as ex:
            self.project.logger.error('Failed to run command', ex)
            return 1

        return 0

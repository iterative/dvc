import dvc.logger as logger
from dvc.command.base import CmdBase
from dvc.exceptions import DvcException


class CmdRun(CmdBase):
    def run(self):
        overwrite = (self.args.yes or self.args.overwrite_dvcfile)

        try:
            self.project.run(cmd=self._parsed_cmd(),
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
        except DvcException:
            logger.error('failed to run command')
            return 1

        return 0

    def _parsed_cmd(self):
        """
        We need to take into account two cases:

        - ['python code.py foo bar']: Used mainly with dvc as a library
        - ['echo', 'foo bar']: List of arguments received from the CLI

        The second case would need quoting, as it was passed through:
                dvc run echo "foo bar"
        """
        if len(self.args.command) < 2:
            return ' '.join(self.args.command)

        return ' '.join(self._quote_argument(arg) for arg in self.args.command)

    def _quote_argument(self, argument):
        if ' ' not in argument or '"' in argument:
            return argument

        return '"{}"'.format(argument)

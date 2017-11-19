from dvc import utils
from dvc.command.common.base import CmdBase, DvcLock
from dvc.executor import ExecutorError
from dvc.logger import Logger


class CmdFind(CmdBase):
    def __init__(self, settings):
        super(CmdFind, self).__init__(settings)

    def run(self):
        with DvcLock(self.is_locker, self.git):
            fname = self.parsed_args.target
            branch_name = self.parsed_args.branch_name

            branches = self.git.branches(branch_name)
            metrics = [self.read_metrics(fname, b) for b in branches]

            zipped = zip(metrics, branches)
            res = self.filter(zipped, self.parsed_args.criteria)
            res = filter(lambda x: x[0] is not None, res)

            for (val, branch) in res:
                if self.parsed_args.show_value:
                    print('{}: {}'.format(branch, val))
                else:
                    print(branch)
            return 0

    @staticmethod
    def filter(zipped, criteria):
        if criteria == 'max':
            res = [max(zipped, key=lambda t: t[0])]
        elif criteria == 'min':
            res = [min(zipped, key=lambda t: t[0])]
        else:
            res = zipped
        return res

    def read_metrics(self, fname, branch):
        try:
            lines = self.git.get_file_content(fname, branch).split('\n')
        except ExecutorError as ex:
            msg = 'Unable to read metrics file from branch {}: {}'
            data_item = self.settings.path_factory.data_item(fname)

            try:
                self.git.get_file_content(data_item.state.relative, branch)
                Logger.error(msg.format(branch, 'this is data file, not metric file'))
            except ExecutorError:
                Logger.error(msg.format(branch, 'file does not exist in this branch'))
                return None

            return None

        metric = utils.parse_target_metric(lines)
        if not metric:
            msg = 'Unable to parse metrics from the first line of file {} in branch {}'
            Logger.error(msg.format(fname, branch))
            return None

        return metric

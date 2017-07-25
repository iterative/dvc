import os

from dvc.command.base import CmdBase, DvcLock
from dvc.logger import Logger
from dvc.runtime import Runtime
from dvc.state_file import StateFile


class CmdTarget(CmdBase):
    def __init__(self, settings):
        super(CmdTarget, self).__init__(settings)

    def define_args(self, parser):
        self.set_no_git_actions(parser)

        parser.add_argument('-u', '--unset', action='store_true', default=False,
                            help='Reset target.')

        parser.add_argument('target', metavar='', help='Target data item.')
        pass

    def run(self):
        with DvcLock(self.is_locker, self.git):
            return self.change_target(self.parsed_args.target, not self.parsed_args.unset)

    def change_target(self, target, unset):
        if target and unset:
            Logger.error('Unable to set target {} and unset it in a single command'.format(target))
            return 1

        if not target and not unset:
            Logger.error('Target is not defined')
            return 1

        target_conf_file_path = self.settings.path_factory.path(self.settings.config.target_file)

        if unset:
            if os.path.exists(target_conf_file_path.relative):
                if not os.path.isfile(target_conf_file_path.relative):
                    Logger.error('Target conf file {} exists but it is not a regular file'.format(
                        target_conf_file_path.relative))
                    return 1
                os.remove(target_conf_file_path.relative)
            open(target_conf_file_path.relative, 'a').close()
        else:
            with open(target_conf_file_path.relative, 'w') as fd:
                target_data_item = self.settings.path_factory.existing_data_item(target)
                fd.write(target_data_item.data_dvc_short)

        return 0


if __name__ == '__main__':
    Runtime.run(CmdTarget, False)

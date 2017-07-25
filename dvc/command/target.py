import os

from dvc.command.base import CmdBase, DvcLock
from dvc.logger import Logger
from dvc.runtime import Runtime


class CmdTarget(CmdBase):
    def __init__(self, settings):
        super(CmdTarget, self).__init__(settings)

    def define_args(self, parser):
        self.set_no_git_actions(parser)
        self.set_reset_flag('-u', '--unset', 'Reset target.')
        parser.add_argument('target', metavar='', nargs='?', help='Target data item.')
        pass

    def run(self):
        with DvcLock(self.is_locker, self.git):
            target = self.parsed_args.target
            unset = self.parsed_args.unset

            if target and unset:
                Logger.error('Unable to set target {} and use --unset in a single command'.format(target))
                return 1

            if not target and not unset:
                Logger.error('Target is not defined')
                return 1

            target_conf_file_path = self.settings.path_factory.path(self.settings.config.target_file).relative
            return self.change_target(target, unset, target_conf_file_path)

    def change_target(self, target, unset, target_conf_file_path):
        if unset:
            return self.unset_target(target_conf_file_path)
        else:
            with open(target_conf_file_path, 'w') as fd:
                target_data_item = self.settings.path_factory.existing_data_item(target)
                fd.write(target_data_item.data.dvc)

        if target:
            msg = 'DVC target: {}'.format(target)
        else:
            msg = 'DVC target unset'
        return self.commit_if_needed(msg)

    @staticmethod
    def unset_target(target_conf_file_path):
        if not os.path.exists(target_conf_file_path):
            Logger.error('Target conf file {} does not exists'.format(
                target_conf_file_path))
            return 1
        if not os.path.isfile(target_conf_file_path):
            Logger.error('Target conf file {} exists but it is not a regular file'.format(
                target_conf_file_path))
            return 1
        if open(target_conf_file_path).read() == '':
            return 0
        os.remove(target_conf_file_path)
        open(target_conf_file_path, 'a').close()
        return 0


if __name__ == '__main__':
    Runtime.run(CmdTarget, False)

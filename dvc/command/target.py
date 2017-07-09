import os

from dvc.command.base import CmdBase, DvcLock
from dvc.logger import Logger


class CmdTarget(CmdBase):
    def __init__(self, settings):
        super(CmdTarget, self).__init__(settings)

    def run(self):
        with DvcLock(self.is_locker, self.git):
            target = self.parsed_args.target_file
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
            target_dvc_path = self.settings.path_factory.existing_data_item(target).data.dvc
            if os.path.exists(target_conf_file_path):
                with open(target_conf_file_path) as fd:
                    if fd.read() == target_dvc_path:
                        return 0
            with open(target_conf_file_path, 'w') as fd:
                fd.write(target_dvc_path)

        return self.commit_if_needed('DVC target: {}'.format(target))

    def unset_target(self, target_conf_file_path):
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

        return self.commit_if_needed('DVC target unset')

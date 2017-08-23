from dvc.cloud.instance_manager import InstanceManager
from dvc.command.base import CmdBase, DvcLock
from dvc.exceptions import DvcException
from dvc.logger import Logger


class CmdInstanceCreate(CmdBase):
    def __init__(self, settings):
        super(CmdInstanceCreate, self).__init__(settings)

    def run(self):
        with DvcLock(self.is_locker, self.git):
            name = self.parsed_args.name
            cloud = self.parsed_args.cloud or self.settings.config.cloud

            # print('NAME: {}'.format(name))
            # print('CLOUD: {}'.format(cloud))

            if not name:
                Logger.error('Instance name is not defined')
                return 1

            try:
                InstanceManager().create(name, cloud, self.parsed_args, self.settings.config)
            except DvcException as ex:
                Logger.error('Instance creation error: {}'.format(ex))
                return 1

            return 0

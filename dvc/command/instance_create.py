import os

from dvc.cloud.instance_manager import InstanceManager
from dvc.command.base import CmdBase, DvcLock
from dvc.logger import Logger


class CmdInstanceCreate(CmdBase):
    def __init__(self, settings):
        super(CmdInstanceCreate, self).__init__(settings)

    def run(self):
        with DvcLock(self.is_locker, self.git):
            name = self.parsed_args.name
            cloud = self.parsed_args.cloud
            type = self.parsed_args.type

            print('NAME: {}'.format(name))
            print('CLOUD: {}'.format(cloud))
            print('TYPE: {}'.format(type))

            if not name:
                Logger.error('Instance name is not defined')
                return 1

            im = InstanceManager()

            if name in set(map(lambda x: x.name, im.instances())):
                Logger.error('Instance with name {} is already exist'.format(name))
                return 1

            instance = im.create(name, cloud, type)

            #return self.commit_if_needed('DVC instance: {}'.format('__EMPTY____'))
            return 0

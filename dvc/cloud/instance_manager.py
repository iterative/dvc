
from dvc.logger import Logger


class InstanceManager(object):
    def instances(self):
        return []

    def create(self, name, cloud, parsed_args, conf_parser):
        if name in set(map(lambda x: x.name, self.instances())):
            Logger.error('Instance with name {} is already exist'.format(name))
            return 1


        pass

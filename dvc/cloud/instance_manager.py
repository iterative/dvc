from dvc.cloud.instance_aws import InstanceAws
from dvc.logger import Logger


class CloudSettings(object):
    def __init__(self, path_factory, global_storage_path, cloud_config):
        self.path_factory = path_factory
        self.cloud_config = cloud_config
        self.global_storage_path = global_storage_path


class InstanceManager(object):
    def instances(self):
        return []

    def create(self, name, cloud, parsed_args, conf_parser):
        if name in set(map(lambda x: x.name, self.instances())):
            Logger.error('Instance with name {} is already exist'.format(name))
            return 1

        inst = InstanceAws()
        pass

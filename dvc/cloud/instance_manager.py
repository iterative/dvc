class CloudSettings(object):
    def __init__(self, cache, global_storage_path, cloud_config):
        self.cache = cache
        self.cloud_config = cloud_config
        self.global_storage_path = global_storage_path

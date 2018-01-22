class CloudSettings(object):
    def __init__(self, cache_dir, global_storage_path, cloud_config):
        self.cache_dir = cache_dir
        self.cloud_config = cloud_config
        self.global_storage_path = global_storage_path

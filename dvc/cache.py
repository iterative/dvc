import os

from dvc.config import Config


class Cache(object):
    CACHE_DIR = 'cache'

    def __init__(self, project):
        from dvc.remote import Remote

        config = project.config._config[Config.SECTION_CACHE]

        local = config.get(Config.SECTION_CACHE_LOCAL, None)
        if local:
            sect = project.config._config[Config.SECTION_REMOTE_FMT.format(local)]
        else:
            sect = {}
            cache_dir = config.get(Config.SECTION_CACHE_DIR, self.CACHE_DIR)
            if not os.path.isabs(cache_dir):
                cache_dir = os.path.abspath(os.path.realpath(os.path.join(project.dvc_dir, cache_dir)))
            sect[Config.SECTION_REMOTE_URL] = cache_dir
            t = config.get(Config.SECTION_CACHE_TYPE, None)
            if t:
                sect[Config.SECTION_CACHE_TYPE] = t

        self.local = Remote(project, sect)

        self.s3 = self._get_remote(project, config, Config.SECTION_CACHE_S3)
        self.gs = self._get_remote(project, config, Config.SECTION_CACHE_GS)
        self.ssh = self._get_remote(project, config, Config.SECTION_CACHE_SSH)
        self.hdfs = self._get_remote(project, config, Config.SECTION_CACHE_HDFS)

    def _get_remote(self, project, config, name):
        from dvc.remote import Remote

        remote = config.get(name, None)
        if not remote:
            return None

        sect = project.config._config[Config.SECTION_REMOTE_FMT.format(remote)]
        return Remote(project, sect)

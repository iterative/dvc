import os

from dvc.remote import Remote


class Cache(object):
    CACHE_DIR = 'cache'

    def __init__(self, project):
        #FIXME
        from dvc.config import Config

        config = project.config._config[Config.SECTION_CACHE]

        local = config.get(Config.SECTION_CACHE_LOCAL, None)
        if local:
            sect = project.config._config[Config.SECTION_REMOTE_FMT.format(local)]
        else:
            sect = {}
            cache_dir = os.path.abspath(os.path.realpath(os.path.join(project.dvc_dir, self.CACHE_DIR)))
            sect[Config.SECTION_REMOTE_URL] = config.get(Config.SECTION_CACHE_DIR, cache_dir)
            t = config.get(Config.SECTION_CACHE_TYPE, None)
            if t:
                sect[Config.SECTION_CACHE_TYPE] = t

        self.local = Remote(project, sect)

        self.s3 = self._get_remote(project, config, Config.SECTION_CACHE_S3)
        self.gs = self._get_remote(project, config, Config.SECTION_CACHE_GS)
        self.ssh = self._get_remote(project, config, Config.SECTION_CACHE_SSH)

    def _get_remote(self, project, config, name):
        remote = config.get(name, None)
        if not remote:
            return None

        sect = project.config._config[Config.SECTION_REMOTE_FMT.format(remote)]
        return Remote(project, sect)

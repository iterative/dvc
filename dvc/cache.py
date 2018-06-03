from dvc.remote.local import RemoteLOCAL
from dvc.remote.s3 import RemoteS3
from dvc.remote.gs import RemoteGS
from dvc.remote.ssh import RemoteSSH


class Cache(object):
    def __init__(self, project):
        #FIXME
        from dvc.config import Config

        config = project.config._config[Config.SECTION_CACHE]

        local = config.get(Config.SECTION_CACHE_LOCAL, None)
        if local:
            sect = project.config._config[Config.SECTION_REMOTE_FMT.format(local)]
        else:
            sect = {}
            d = config.get(Config.SECTION_CACHE_DIR, None)
            if d:
                sect[Config.SECTION_REMOTE_URL] = d
            t = config.get(Config.SECTION_CACHE_TYPE, None)
            if t:
                sect[Config.SECTION_CACHE_TYPE] = t

        self.local = RemoteLOCAL(project, sect)

        self.s3 = None
        s3 = config.get(Config.SECTION_CACHE_S3, None)
        if s3:
            sect = project.config._config[Config.SECTION_REMOTE_FMT.format(s3)]
            self.s3 = RemoteS3(project, sect)

        self.gs = None
        gs = config.get(Config.SECTION_CACHE_GS, None)
        if gs:
            sect = project.config._config[Config.SECTION_REMOTE_FMT.format(gs)]
            self.gs = RemoteGS(project, sect)

#        self.ssh = None
#        ssh = config.get(Config.SECTION_CACHE_SSH, None)
#        if ssh:
#            sect = project.config._config[Config.SECTION_REMOTE_FMT.format(ssh)]
#            self.ssh = RemoteSSH(project, sect)

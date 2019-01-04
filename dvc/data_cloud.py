import dvc.logger as logger
from dvc.config import Config, ConfigError

from dvc.remote import Remote
from dvc.remote.s3 import RemoteS3
from dvc.remote.gs import RemoteGS
from dvc.remote.azure import RemoteAzure
from dvc.remote.ssh import RemoteSSH
from dvc.remote.hdfs import RemoteHDFS
from dvc.remote.local import RemoteLOCAL
from dvc.remote.http import RemoteHTTP


class DataCloud(object):
    CLOUD_MAP = {
        'aws': RemoteS3,
        'gcp': RemoteGS,
        'azure': RemoteAzure,
        'ssh': RemoteSSH,
        'hdfs': RemoteHDFS,
        'local': RemoteLOCAL,
        'http': RemoteHTTP,
        'https': RemoteHTTP,
    }

    def __init__(self, project, config=None):
        self.project = project
        self._config = config
        self._core = self._config[Config.SECTION_CORE]

    @property
    def _cloud(self):
        remote = self._core.get(Config.SECTION_CORE_REMOTE, '')
        if remote != '':
            return self._init_remote(remote)

        if self._core.get(Config.SECTION_CORE_CLOUD, None):
            # backward compatibility
            msg = 'using obsoleted config format. Consider updating.'
            logger.warning(msg)
            return self._init_compat()

        return None

    def _init_remote(self, remote):
        section = Config.SECTION_REMOTE_FMT.format(remote)
        cloud_config = self._config.get(section, None)
        if not cloud_config:
            msg = "can't find remote section '{}' in config"
            raise ConfigError(msg.format(section))

        return Remote(self.project, cloud_config)

    def _init_compat(self):
        name = self._core.get(Config.SECTION_CORE_CLOUD, '').strip().lower()
        if name == '':
            self._cloud = None
            return

        cloud_type = self.CLOUD_MAP.get(name, None)
        if not cloud_type:
            msg = "wrong cloud type '{}' specified".format(name)
            raise ConfigError(msg)

        cloud_config = self._config.get(name, None)
        if not cloud_config:
            msg = "can't find cloud section '{}' in config".format(name)
            raise ConfigError(msg)

        # NOTE: check if the class itself has everything needed for operation.
        # E.g. all the imported packages.
        if not cloud_type.supported(cloud_type.compat_config(cloud_config)):
            raise ConfigError("unsupported cloud '{}'".format(name))

        return self._init_cloud(cloud_config, cloud_type)

    def _init_cloud(self, cloud_config, cloud_type):
        global_storage_path = self._core.get(Config.SECTION_CORE_STORAGEPATH)
        if global_storage_path:
            logger.warning('using obsoleted config format. Consider updating.')

        cloud = cloud_type(self.project, cloud_config)
        return cloud

    def _get_cloud(self, remote, cmd):
        if remote:
            return self._init_remote(remote)

        if self._cloud:
            return self._cloud

        raise ConfigError(
            'No remote repository specified. Setup default repository with\n'
            '    dvc config core.remote <name>\n'
            'or use:\n'
            '    dvc {} -r <name>\n'
            .format(cmd)
        )

    def push(self, targets, jobs=1, remote=None, show_checksums=False):
        """Push data items in a cloud-agnostic way."""
        return self.project.cache.local.push(targets,
                                             jobs=jobs,
                                             remote=self._get_cloud(remote,
                                                                    'push'),
                                             show_checksums=show_checksums)

    def pull(self, targets, jobs=1, remote=None, show_checksums=False):
        """Pull data items in a cloud-agnostic way."""
        return self.project.cache.local.pull(targets,
                                             jobs=jobs,
                                             remote=self._get_cloud(remote,
                                                                    'pull'),
                                             show_checksums=show_checksums)

    def status(self, targets, jobs=1, remote=None, show_checksums=False):
        """Check status of data items in a cloud-agnostic way."""
        cloud = self._get_cloud(remote, 'status')
        return self.project.cache.local.status(targets,
                                               jobs=jobs,
                                               remote=cloud,
                                               show_checksums=show_checksums)

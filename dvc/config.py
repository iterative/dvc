import os
import configparser

from dvc.exceptions import DvcException
from dvc.logger import Logger


class ConfigError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Config file error: {}'.format(msg))


class ConfigI(object):
    def __init__(self, data_dir=None, cache_dir=None, state_dir=None):
        self.set(data_dir, cache_dir, state_dir)

    def set(self, data_dir, cache_dir, state_dir):
        self._data_dir = data_dir
        self._cache_dir = cache_dir
        self._state_dir = state_dir

    @property
    def data_dir(self):
        return self._data_dir

    @property
    def cache_dir(self):
        return self._cache_dir

    @property
    def state_dir(self):
        return self._state_dir

    @property
    def storage_prefix(self):
        return ''

    def sanity_check(self):
        pass

    def aws_access_key_id(self):
        return None

    @property
    def aws_secret_access_key(self):
        return None


class Config(ConfigI):
    CONFIG = 'dvc.conf'
    def __init__(self, conf_file, conf_pseudo_file=None):
        """
        Params:
            conf_file (String): configuration file
            conf_pseudo_file (String): for unit testing, something that supports readline; supersedes conf_file
        """
        self._conf_file = conf_file
        self._config = configparser.SafeConfigParser()

        if conf_pseudo_file is not None:
            self._config.readfp(conf_pseudo_file)
        else:
            if not os.path.isfile(conf_file):
                raise ConfigError('Config file "{}" does not exist'.format(conf_file))
            self._config.read(conf_file)

        level = self._config['Global']['LogLevel']
        Logger.set_level(level)

        self._aws_creds = None

        super(Config, self).__init__(self._config['Global']['DataDir'],
                                     self._config['Global']['CacheDir'],
                                     self._config['Global']['StateDir'])
        pass

    @property
    def file(self):
        return self._conf_file

    @property
    def aws_access_key_id(self):
        if not self._aws_creds:
            self._aws_creds = self.get_aws_credentials()
        if not self._aws_creds:
            return None
        return self._aws_creds[0]

    @property
    def aws_secret_access_key(self):
        if not self._aws_creds:
            self._aws_creds = self.get_aws_credentials()
        if not self._aws_creds:
            return None
        return self._aws_creds[1]

    @property
    def aws_region_host(self):
        """ get the region host needed for s3 access

        See notes http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
        """

        region = self._config['AWS']['Region']
        if region is None or region == '':
            return 's3.amazonaws.com'
        if region == 'us-east-1':
            return 's3.amazonaws.com'
        return 's3.%s.amazonaws.com' % region


    @property
    def storage_path(self):
        """ get storage path

        Precedence: Storage, then cloud specific
        """

        path = self._config['Global'].get('StoragePath', None)
        if path:
            return path

        cloud = self.cloud
        assert cloud in ['aws', 'gcp'], 'unknown cloud %s' % cloud
        if cloud == 'aws':
            path = self._config['AWS'].get('StoragePath', None)
        elif cloud == 'gcp':
            path = self._config['GCP'].get('StoragePath', None)
        if path is None:
            raise ConfigError('invalid StoragePath: not set for Data or cloud specific')
        return path

    def _storage_path_parts(self):
        return self.storage_path.strip('/').split('/', 1)

    @property
    def storage_bucket(self):
        """ Data -> StoragePath takes precedence; if doesn't exist, use cloud-specific """
        return self._storage_path_parts()[0]

    @property
    def storage_prefix(self):
        parts = self._storage_path_parts()
        if len(parts) > 1:
            return parts[1]
        return ''

    @property
    def gc_project_name(self):
        return self._config['GCP']['ProjectName']

    @property
    def cloud(self):
        """ get cloud choice: currently one of ['AWS', 'GCP'] """
        conf = self._config['Global'].get('Cloud', '')
        if conf.lower() in ['aws']:
            return 'aws'
        if conf.lower() in ['gcp']:
            return 'gcp'
        return conf


    def get_aws_credentials(self):
        """ gets aws credentials, looking in various places

        Params:

        Searches:
        1 any override in dvc.conf [AWS] CredentialPath;
        2 ~/.aws/credentials


        Returns:
            if successfully found, (access_key_id, secret)
            None otherwise
        """
        default = os.path.expanduser('~/.aws/credentials')

        paths = []
        credpath = self._config['AWS'].get('CredentialPath', None)
        if credpath is not None and len(credpath) > 0:
            credpath = os.path.expanduser(credpath)
            if os.path.isfile(credpath):
                paths.append(credpath)
            else:
                Logger.warn('AWS CredentialPath "%s" not found; falling back to default "%s"' % (credpath, default))
                paths.append(default)
        else:
            paths.append(default)

        for path in paths:
            cc = configparser.SafeConfigParser()
            threw = False
            try:
                # use readfp(open( ... to aid mocking.
                cc.readfp(open(path, 'r'))
            except Exception as e:
                threw = True
            if not threw and 'default' in cc.keys():
                access_key = cc['default'].get('aws_access_key_id', None)
                secret = cc['default'].get('aws_secret_access_key', None)

                if access_key is not None and secret is not None:
                    return (access_key, secret)

        return None

    def sanity_check(self):
        """ sanity check a config

        check that we have a cloud and storagePath
        if aws, check can read credentials
        if google, check ProjectName

        Returns:
            (T,) if good
            (F, issues) if bad
        """
        errors = []
        for key in ['Cloud']:
            if key.lower() not in self._config['Global'].keys() or len(self._config['Global'][key]) < 1:
                errors.append('Please set %s in section Global in config file %s' % (key, self.file))

        # now that a cloud is chosen, can check StoragePath
        sp = self.storage_path
        if sp is None or len(sp) == 0:
            errors.append('Please set StoragePath = bucket/{optional path} in conf file "%s" '
                           'either in Global or a cloud specific section' % self.CONFIG)

        cloud = self.cloud
        if cloud == '':
            # not set; already complained above
            pass
        elif cloud == 'aws':
            creds = self.get_aws_credentials()
            if creds is None:
                errors.append('can\'t find aws credentials.')
            self._aws_creds = creds
        elif cloud == 'gcp':
            project = self._config['GCP'].get('ProjectName', None)
            if project is None or len(project) < 1:
                errors.append('can\'t read google cloud project name. Please set ProjectName in section GC.')
        else:
            errors.append('unknown Cloud %s' % cloud)

        return (len(errors) == 0, errors)




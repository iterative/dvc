import base64
import hashlib
import os
import threading
import configparser
import shutil

from boto.s3.connection import S3Connection
from google.cloud import storage as gc

from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.config import ConfigError
from dvc.settings import Settings


class DataCloudError(DvcException):
    def __init__(self, msg):
        super(DataCloudError, self).__init__('Data sync error: {}'.format(msg))


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)


def percent_cb(complete, total):
    Logger.debug('{} transferred out of {}'.format(sizeof_fmt(complete), sizeof_fmt(total)))


def file_md5(fname):
    """ get the (md5 hexdigest, md5 digest) of a file """
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1000), b""):
            hash_md5.update(chunk)
    return (hash_md5.hexdigest(), hash_md5.digest())


class DataCloudBase(object):
    """ Base class for DataCloud """
    def __init__(self, settings, config, cloud_config):
        self._settings = settings
        self._config = config
        self._cloud_config = cloud_config
        self._lock = threading.Lock()

    @property
    def storage_path(self):
        """ get storage path

        Precedence: Storage, then cloud specific
        """

        path = self._config['Global'].get('StoragePath', None)
        if path:
            return path

        path = self._cloud_config.get('StoragePath', None)
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

    def cache_file_key(self, file):
        return '{}/{}'.format(self.storage_prefix, file).strip('/')

    def sanity_check(self):
        pass

    def sync_to_cloud(self, item):
        pass

    def sync_from_cloud(self, item):
        pass

    def sync(self, fname):
        item = self._settings.path_factory.data_item(fname)
        if os.path.isfile(item.resolved_cache.dvc):
            self.sync_to_cloud(item)
        else:
            self.create_directory(fname, item)
            self.sync_from_cloud(item)

    def create_directory(self, fname, item):
        self._lock.acquire()
        try:
            dir = os.path.dirname(item.cache.relative)
            if not os.path.exists(dir):
                Logger.debug(u'Creating directory {}'.format(dir))
                try:
                    os.makedirs(dir)
                except OSError as ex:
                    raise DataCloudError(u'Cannot create directory {}: {}'.format(dir, ex))
            elif not os.path.isdir(dir):
                msg = u'File {} cannot be synced because {} is not a directory'
                raise DataCloudError(msg.format(fname, dir))
        finally:
            self._lock.release()

    def remove_from_cloud(self, item):
        pass


class DataCloudLOCAL(DataCloudBase):
    def sync_to_cloud(self, item):
        Logger.debug('sync to cloud ' + item.resolved_cache.dvc + " " + self.storage_path)
        shutil.copy(item.resolved_cache.dvc, self.storage_path)

    def sync_from_cloud(self, item):
        Logger.debug('sync from cloud ' + self.storage_path + " " + item.resolved_cache.dvc)
        shutil.copy(self.storage_path, item.resolved_cache.dvc)

    def remove_from_cloud(self, item):
        Logger.debug('rm from cloud ' + item.resolved_cache.dvc)
        os.remove(item.resolved_cache.dvc)


class DataCloudAWS(DataCloudBase):
    """ DataCloud class for Amazon Web Services """
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

        region = self._cloud_config['Region']
        if region is None or region == '':
            return 's3.amazonaws.com'
        if region == 'us-east-1':
            return 's3.amazonaws.com'
        return 's3.%s.amazonaws.com' % region

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
        credpath = self._cloud_config.get('CredentialPath', None)
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
        creds = self.get_aws_credentials()
        if creds is None:
            raise ConfigError('can\'t find aws credentials.')
        self._aws_creds = creds

    def _get_bucket_aws(self):
        """ get a bucket object, aws """
        conn = S3Connection(self.aws_access_key_id, self.aws_secret_access_key, host=self.aws_region_host)
        bucket_name = self.storage_bucket
        bucket = conn.lookup(bucket_name)
        if bucket is None:
            raise DataCloudError('Storage path is not setup correctly')
        return bucket

    def sync_from_cloud(self, item):
        """ sync from cloud, aws version """

        bucket = self._get_bucket_aws()

        key_name = self.cache_file_key(item.resolved_cache.dvc)
        key = bucket.get_key(key_name)
        if not key:
            raise DataCloudError('File "{}" does not exist in the cloud'.format(key_name))

        Logger.info('Downloading cache file from S3 "{}/{}"'.format(bucket.name, key_name))
        key.get_contents_to_filename(item.resolved_cache.relative, cb=percent_cb)
        Logger.info('Downloading completed')

    def sync_to_cloud(self, data_item):
        """ sync_to_cloud, aws version """

        aws_key = self.cache_file_key(data_item.resolved_cache.dvc)
        bucket = self._get_bucket_aws()
        key = bucket.get_key(aws_key)
        if key:
            Logger.debug('File already uploaded to the cloud. Checksum validation...')

            md5_cloud = key.etag[1:-1]
            md5_local = file_md5(data_item.resolved_cache.dvc)[0]
            if md5_cloud == md5_local:
                Logger.debug('File checksum matches. No uploading is needed.')
                return

            Logger.debug('Checksum miss-match. Re-uploading is required.')

        Logger.info('Uploading cache file "{}" to S3 "{}"'.format(data_item.resolved_cache.relative, aws_key))
        key = bucket.new_key(aws_key)
        key.set_contents_from_filename(data_item.resolved_cache.relative, cb=percent_cb)
        Logger.info('Uploading completed')

    def remove_from_cloud(self, data_item):
        aws_file_name = self.cache_file_key(data_item.cache.dvc)

        Logger.debug(u'[Cmd-Remove] Remove from cloud {}.'.format(aws_file_name))

        if not self.aws_access_key_id or not self.aws_secret_access_key:
            Logger.debug('[Cmd-Remove] Unable to check cache file in the cloud')
            return
        conn = S3Connection(self.aws_access_key_id, self.aws_secret_access_key)
        bucket_name = self.storage_bucket
        bucket = conn.lookup(bucket_name)
        if bucket:
            key = bucket.get_key(aws_file_name)
            if not key:
                Logger.warn('[Cmd-Remove] S3 remove warning: file "{}" does not exist in S3'.format(aws_file_name))
            else:
                key.delete()
                Logger.info('[Cmd-Remove] File "{}" was removed from S3'.format(aws_file_name))
        pass


class DataCloudGCP(DataCloudBase):
    """ DataCloud class for Google Cloud Platform """
    @property
    def gc_project_name(self):
        return self._cloud_config.get('ProjectName', None)

    def sanity_check(self):
        project = self.gc_project_name
        if project is None or len(project) < 1:
            raise ConfigError('can\'t read google cloud project name. Please set ProjectName in section GC.')

    def _get_bucket_gc(self):
        """ get a bucket object, gc """
        client = gc.Client(project=self.gc_project_name)
        bucket = client.bucket(self.storage_bucket)
        if not bucket.exists():
            raise DataCloudError('sync up: google cloud bucket {} doesn\'t exist'.format(self.storage_bucket))
        return bucket

    def sync_from_cloud(self, item):
        """ sync from cloud, gcp version """

        bucket = self._get_bucket_gc()
        key = self.cache_file_key(item.resolved_cache.dvc)

        blob = bucket.get_blob(key)
        if not blob:
            raise DataCloudError('File "{}" does not exist in the cloud'.format(key))

        Logger.info('Downloading cache file from gc "{}/{}"'.format(bucket.name, key))

        blob.download_to_filename(item.resolved_cache.dvc)
        Logger.info('Downloading completed')

    def sync_to_cloud(self, data_item):
        """ sync_to_cloud, gcp version """

        bucket = self._get_bucket_gc()
        blob_name = self.cache_file_key(data_item.resolved_cache.dvc)

        blob = bucket.get_blob(blob_name)
        if blob is not None and blob.exists():
            b64_encoded_md5 = base64.b64encode(file_md5(data_item.resolved_cache.dvc)[1])

            if blob.md5_hash == b64_encoded_md5:
                Logger.debug('checksum %s matches.  Skipping upload' % data_item.cache.relative)
                return
            Logger.debug('checksum %s mismatch.  re-uploading' % data_item.cache.relative)

        Logger.info('uploading cache file "{} to gc "{}"'.format(data_item.cache.relative, blob_name))

        blob = bucket.blob(blob_name)
        blob.upload_from_filename(data_item.resolved_cache.relative)
        Logger.info('uploading %s completed' % data_item.resolved_cache.relative)

    def remove_from_cloud(self, item):
        raise Exception('NOT IMPLEMENTED YET')


class DataCloud(object):
    """ Generic class to do initial config parsing and redirect to proper DataCloud methods """

    CLOUD_MAP = {
        'AWS': DataCloudAWS,
        'GCP': DataCloudGCP,
        'LOCAL': DataCloudLOCAL,
    }

    def __init__(self, settings):
        assert isinstance(settings, Settings) 

        #To handle ConfigI case
        if not hasattr(settings.config, '_config'):
            self._cloud = DataCloudBase(None, None, None)
            return

        self._settings = settings
        self._config = self._settings.config._config

        self.typ = self._config['Global'].get('Cloud', '').strip().upper()
        if self.typ not in self.CLOUD_MAP.keys():
            raise ConfigError('Wrong cloud type %s specified' % self.typ)

        if self.typ not in self._config.keys():
            raise ConfigError('Can\'t find cloud section \'[%s]\' in config' % self.typ)

        self._cloud = self.CLOUD_MAP[self.typ](self._settings, self._config, self._config[self.typ])

        self.sanity_check()

    def sanity_check(self):
        """ sanity check a config

        check that we have a cloud and storagePath
        if aws, check can read credentials
        if google, check ProjectName

        Returns:
            (T,) if good
            (F, issues) if bad
        """
        for key in ['Cloud']:
            if key.lower() not in self._config['Global'].keys() or len(self._config['Global'][key]) < 1:
                raise ConfigError('Please set %s in section Global in config file %s' % (key, self.file))

        # now that a cloud is chosen, can check StoragePath
        sp = self._cloud.storage_path
        if sp is None or len(sp) == 0:
            raise ConfigError('Please set StoragePath = bucket/{optional path} in conf file "%s" '
                           'either in Global or a cloud specific section' % self.CONFIG)

        self._cloud.sanity_check()

    def sync(self, fname):
        return self._cloud.sync(fname)

    def remove_from_cloud(self, item):
        return self._cloud.remove_from_cloud(item)

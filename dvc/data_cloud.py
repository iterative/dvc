import base64
import hashlib
import os
import threading
import configparser
import tempfile
import requests
import configparser
import filecmp
import math

from boto.s3.connection import S3Connection
from google.cloud import storage as gc

try:
    from urlparse import urlparse
except ImportError:
    # Python 3
    from urllib.parse import urlparse

import dvc
from dvc.cloud.instance_manager import CloudSettings
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.config import ConfigError
from dvc.progress import progress
from dvc.utils import copyfile
from dvc.cloud.credentials_aws import AWSCredentials
from dvc.utils import cached_property
from dvc.system import System
from dvc.utils import map_progress


STATUS_UNKNOWN  = 0
STATUS_OK       = 1
STATUS_MODIFIED = 2
STATUS_NEW      = 3
STATUS_DELETED  = 4

STATUS_MAP = {
    # (local_exists, remote_exists, cmp)
    (True, True, True)  : STATUS_OK,
    (True, True, False) : STATUS_MODIFIED,
    (True, False, None) : STATUS_NEW,
    (False, True, None) : STATUS_DELETED,
}

class DataCloudError(DvcException):
    def __init__(self, msg):
        super(DataCloudError, self).__init__('Data sync error: {}'.format(msg))


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)


def percent_cb(name, part_complete, part_total, offset=0, multipart_total=None):
    complete = offset + part_complete
    total = multipart_total if multipart_total != None else part_total

    Logger.debug('{}: {} transferred out of {}'.format(
                                    name,
                                    sizeof_fmt(complete),
                                    sizeof_fmt(total)))
    progress.update_target(os.path.basename(name), complete, total)


def create_cb(name, offset=0, multipart_total=None):
    return (lambda cur,tot: percent_cb(name, cur, tot, offset, multipart_total))


def file_md5(fname):
    """ get the (md5 hexdigest, md5 digest) of a file """
    if os.path.exists(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(1024*1000), b""):
                hash_md5.update(chunk)
        return (hash_md5.hexdigest(), hash_md5.digest())
    else:
        return (None, None)


class DataCloudBase(object):
    """ Base class for DataCloud """
    def __init__(self, cloud_settings):
        self._cloud_settings = cloud_settings
        self._lock = threading.Lock()

    @property
    def storage_path(self):
        """ get storage path

        Precedence: Storage, then cloud specific
        """

        if self._cloud_settings.global_storage_path:
            return self._cloud_settings.global_storage_path

        path = self._cloud_settings.cloud_config.get('StoragePath', None)
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

    def tmp_file(self, fname):
        return fname + '.part'

    def sanity_check(self):
        pass

    def _import(self, bucket, path, fname, item):
        pass

    def push(self, item):
        pass

    def pull(self, item):
        fname = item.resolved_cache.dvc
        key_name = self.cache_file_key(fname)

        return self._import(self.storage_bucket, key_name, fname, item)

    def import_data(self, url, item):
        o = urlparse(url)

        return self._import(o.netloc, o.path, item.cache.relative, item)

    def sync(self, fname):
        item = self._cloud_settings.path_factory.data_item(fname)

        if os.path.isfile(item.resolved_cache.dvc):
            return self.push(item)
        else:
            self.create_directory(item)
            return self.pull(item)

    def create_directory(self, item):
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
                raise DataCloudError(msg.format(item.cache.relative, dir))
        finally:
            self._lock.release()

    def remove(self, item):
        pass

    def _status(self, item):
        pass

    def status(self, item):
        return STATUS_MAP.get(self._status(item), STATUS_UNKNOWN)

class DataCloudLOCAL(DataCloudBase):
    def push(self, item):
        Logger.debug('sync to cloud ' + item.resolved_cache.dvc + " " + self.storage_path)
        copyfile(item.resolved_cache.dvc, self.storage_path)
        return item

    def _import(self, i, out, item):
        tmp_file = self.tmp_file(out)
        try:
            copyfile(i, tmp_file)
            os.rename(tmp_file, out)
        except Exception as exc:
            Logger.error('Failed to copy "{}": {}'.format(i, exc))
            return None

        return item

    def pull(self, item):
        Logger.debug('sync from cloud ' + self.storage_path + " " + item.resolved_cache.dvc)
        return self._import(self.storage_path, item.resolved_cache.dvc, item)

    def remove(self, item):
        Logger.debug('rm from cloud ' + item.resolved_cache.dvc)
        os.remove(item.resolved_cache.dvc)

    def import_data(self, path, item):
        Logger.debug('import from cloud ' + path + " " + item.cache.relative)
        return self._import(path, item.cache.relative, item)

    def _status(self, data_item):
        local = data_item.resolved_cache.relative
        remote = '{}/{}'.format(self.storage_path, os.path.basename(local))

        remote_exists = os.path.exists(remove)
        local_exists = os.path.exists(local)
        c = None
        if local_exists and remote_exists:
            c = filecmp.cmp(local, remote)

        return (local_exists, remote_exists, c)

class DataCloudHTTP(DataCloudBase):
    def push(self, item):
        raise Exception('Not implemented yet')

    def pull(self, item):
        raise Exception('Not implemented yet')

    def remove(self, item):
        raise Exception('Not implemented yet')

    def status(self, item):
        raise Exception('Not implemented yet')

    def _downloaded_size(self, fname):
        if os.path.exists(fname) and self.parsed_args.cont:
            downloaded = os.path.getsize(fname)
            header = {'Range': 'bytes=%d-' % downloaded}

            Logger.debug('found existing {} file, resuming download'.format(tmp_file))

            return (downloaded, header)

        return (0, None)

    def _get_header(self, r, name):
        val = r.headers.get(name)
        if val == None:
            Logger.debug('\'{}\' not supported by the server'.format(name))

        return val

    def _verify_downloaded_size(self, r, downloaded_size):
        content_range = self._get_header(r, 'content-range')
        if downloaded_size and content_range == None:
            Logger.debug('Can\'t resume download')
            return 0

        return downloaded_size

    def _download(self, r, fname, downloaded):
        mode = 'ab' if downloaded else 'wb'
        name = os.path.basename(r.url)
        total_length = self._get_header(r, 'content-length')
        chunk_size = 1024 * 100

        progress.update_target(name, downloaded, total_length)

        with open(fname, mode) as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if not chunk:  # filter out keep-alive new chunks
                    continue

                f.write(chunk)
                downloaded += len(chunk)
                progress.update_target(name, downloaded, total_length)


        progress.finish_target(name)

    def _verify_md5(self, r, fname):
        md5 = file_md5(fname)[0]
        content_md5 = self._get_header(r, 'content-md5')

        if content_md5 == None:
            return True

        if md5 != content_md5:
            Logger.error('Checksum mismatch')
            return False

        Logger.debug('Checksum matches')
        return True

    def import_data(self, url, item):
        """
        Download single file from url.
        """

        to_file = item.cache.relative
        tmp_file = self.tmp_file(to_file)

        downloaded, header = self._downloaded_size(tmp_file)
        r = requests.get(url, stream=True, headers=header)
        downloaded = self._verify_downloaded_size(r, downloaded)

        try:
            self._download(r, tmp_file, downloaded)
        except Exception as exc:
            Logger.error('Failed to download "{}": {}'.format(url, exc))
            return None

        if not self._verify_md5(r, tmp_file):
            return None

        os.rename(tmp_file, to_file)

        return item


class DataCloudAWS(DataCloudBase):
    """ DataCloud class for Amazon Web Services """
    def __init__(self, cloud_settings): # settings, config, cloud_config):
        super(DataCloudAWS, self).__init__(cloud_settings)
        self._aws_creds = AWSCredentials(cloud_settings.cloud_config)

    @property
    def aws_region_host(self):
        """ get the region host needed for s3 access

        See notes http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
        """

        region = self._cloud_settings.cloud_config['Region']
        if region is None or region == '':
            return 's3.amazonaws.com'
        if region == 'us-east-1':
            return 's3.amazonaws.com'
        return 's3.%s.amazonaws.com' % region

    def credential_paths(self, default):
        paths = []
        credpath = self._cloud_settings.cloud_config.get('CredentialPath', None)
        if credpath is not None and len(credpath) > 0:
            credpath = os.path.expanduser(credpath)
            if os.path.isfile(credpath):
                paths.append(credpath)
            else:
                Logger.warn('AWS CredentialPath "%s" not found; falling back to default "%s"' % (credpath, default))
                paths.append(default)
        else:
            paths.append(default)
        return paths

    def _get_bucket_aws(self, bucket_name):
        """ get a bucket object, aws """
        if all([self._aws_creds.access_key_id, self._aws_creds.secret_access_key, self.aws_region_host]):
            conn = S3Connection(self._aws_creds.access_key_id,
                                self._aws_creds.secret_access_key,
                                host=self.aws_region_host)
        else:
            conn = S3Connection()
        bucket = conn.lookup(bucket_name)
        if bucket is None:
            raise DataCloudError('Storage path {} is not setup correctly'.format(bucket_name))
        return bucket

    def _cmp_checksum(self, key, fname):
        md5_cloud = key.metadata.get('dvc-md5', None)
        md5_local = file_md5(fname)[0]

        if md5_cloud == None:
            md5_cloud = key.etag[1:-1]

        if md5_cloud == md5_local:
            return True

        return False

    def _import(self, bucket_name, key_name, fname, data_item):

        bucket = self._get_bucket_aws(bucket_name)

        tmp_file = self.tmp_file(fname)
        name = os.path.basename(fname)
        key = bucket.get_key(key_name)
        if not key:
            Logger.error('File "{}" does not exist in the cloud'.format(key_name))
            return None

        if self._cmp_checksum(key, fname):
            Logger.debug('File "{}" matches with "{}".'.format(fname, key_name))
            return data_item

        Logger.debug('Downloading cache file from S3 "{}/{}" to "{}"'.format(bucket.name, key_name, fname))

        temp_file = None
        try:
            key.get_contents_to_filename(tmp_file, cb=create_cb(name))
            os.rename(tmp_file, fname)
        except Exception as exc:
            Logger.error('Failed to download "{}": {}'.format(key_name, exc))
            return None

        progress.finish_target(name)
        Logger.debug('Downloading completed')

        return data_item

    def _push_multipart(self, key, fname):
        # AWS doesn't provide easilly accessible md5 for multipart
        # objects, so we have to store our own md5 sum to use later.
        metadata = {'dvc-md5' : str(file_md5(fname)[0])}

        mp = key.bucket.initiate_multipart_upload(key.name, metadata=metadata)

        source_size = os.stat(fname).st_size
        chunk_size = 50*1024*1024
        chunk_count = int(math.ceil(source_size / float(chunk_size)))

        with open(fname, 'rb') as fp:
            for i in range(chunk_count):
                offset = i * chunk_size
                left = source_size - offset
                size = min([chunk_size, left])
                part_num = i + 1

                fp.seek(offset)
                mp.upload_part_from_file(fp=fp,
                                         size=size,
                                         num_cb=100,
                                         part_num=part_num,
                                         cb=create_cb(fname, offset, source_size))

        if len(mp.get_all_parts()) != chunk_count:
            raise Exception("Couldn't upload all file parts")

        mp.complete_upload()

    def push(self, data_item):
        """ push, aws version """

        aws_key = self.cache_file_key(data_item.resolved_cache.dvc)
        bucket = self._get_bucket_aws(self.storage_bucket)
        key = bucket.get_key(aws_key)
        if key:
            Logger.debug('File already uploaded to the cloud. Checksum validation...')

            if self._cmp_checksum(key, data_item.resolved_cache.dvc):
                Logger.debug('File checksum matches. No uploading is needed.')
                return data_item

            Logger.debug('Checksum miss-match. Re-uploading is required.')

        key = bucket.new_key(aws_key)

        try:
            self._push_multipart(key, data_item.resolved_cache.relative)
        except Exception as exc:
            Logger.error('Failed to upload "{}": {}'.format(data_item.resolved_cache.relative, exc))
            return None

        progress.finish_target(os.path.basename(data_item.resolved_cache.relative))

        return data_item

    def _status(self, data_item):
        aws_key = self.cache_file_key(data_item.resolved_cache.dvc)
        bucket = self._get_bucket_aws(self.storage_bucket)
        key = bucket.get_key(aws_key)

        remote_exists = key is not None
        local_exists = os.path.exists(data_item.resolved_cache.relative)
        c = None
        if remote_exists and local_exists:
            c = self._cmp_checksum(key, data_item.resolved_cache.dvc)

        return (local_exists, remote_exists, c)

    def remove(self, data_item):
        aws_file_name = self.cache_file_key(data_item.cache.dvc)

        Logger.debug(u'[Cmd-Remove] Remove from cloud {}.'.format(aws_file_name))

        if not self._aws_creds.access_key_id or not self._aws_creds.secret_access_key:
            Logger.debug('[Cmd-Remove] Unable to check cache file in the cloud')
            return
        conn = S3Connection(self._aws_creds.access_key_id, self._aws_creds.secret_access_key)
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

    def _get_bucket_gc(self, storage_bucket):
        """ get a bucket object, gc """
        client = gc.Client(project=self.gc_project_name)
        bucket = client.bucket(storage_bucket)
        if not bucket.exists():
            raise DataCloudError('sync up: google cloud bucket {} doesn\'t exist'.format(self.storage_bucket))
        return bucket

    def _cmp_checksum(self, blob, fname):
        b64_encoded_md5 = base64.b64encode(file_md5(fname)[1])

        if blob.md5_hash == b64_encoded_md5:
            return True

        return False

    def _import(self, bucket_name, key, fname, data_item):

        bucket = self._get_bucket_gc(bucket_name)

        name = os.path.basename(fname)
        tmp_file = self.tmp_file(fname)

        blob = bucket.get_blob(key)
        if not blob:
            Logger.error('File "{}" does not exist in the cloud'.format(key))
            return None

        Logger.info('Downloading cache file from gc "{}/{}"'.format(bucket.name, key))

        # percent_cb is not available for download_to_filename, so
        # lets at least update progress at keypoints(start, finish)
        progress.update_target(name, 0, None)

        try:
            blob.download_to_filename(tmp_file)
            os.rename(tmp_file, fname)
        except Exception as exc:
            Logger.error('Failed to download "{}": {}'.format(key, exc))
            return None

        progress.finish_target(name)

        Logger.info('Downloading completed')

        return data_item

    def push(self, data_item):
        """ push, gcp version """

        bucket = self._get_bucket_gc(self.storage_bucket)
        blob_name = self.cache_file_key(data_item.resolved_cache.dvc)
        name = os.path.basename(data_item.resolved_cache.dvc)

        blob = bucket.get_blob(blob_name)
        if blob is not None and blob.exists():
            if self._cmp_checksum(blob, data_item.resolved_cache.dvc):
                Logger.debug('checksum %s matches.  Skipping upload' % data_item.cache.relative)
                return data_item
            Logger.debug('checksum %s mismatch.  re-uploading' % data_item.cache.relative)

        # same as in _import
        progress.update_target(name, 0, None)

        blob = bucket.blob(blob_name)
        blob.upload_from_filename(data_item.resolved_cache.relative)

        progress.finish_target(name)
        Logger.info('uploading %s completed' % data_item.resolved_cache.relative)

        return data_item

    def _status(self, data_item):
        """ status, gcp version """

        bucket = self._get_bucket_gc(self.storage_bucket)
        blob_name = self.cache_file_key(data_item.resolved_cache.dvc)
        blob = bucket.get_blob(blob_name)

        remote_exists = blob is not None and blob.exists()
        local_exists = os.path.exists(data_item.resolved_cache.relative)
        c = None
        if remote_exists and local_exists:
            c = self._cmp_checksum(blob, data_item.resolved_cache.dvc)

        return (local_exists, remote_exists, c)

    def remove(self, item):
        raise Exception('NOT IMPLEMENTED YET')


class DataCloud(object):
    """ Generic class to do initial config parsing and redirect to proper DataCloud methods """

    CLOUD_MAP = {
        'AWS'   : DataCloudAWS,
        'GCP'   : DataCloudGCP,
        'HTTP'  : DataCloudHTTP,
        'LOCAL' : DataCloudLOCAL,
    }

    SCHEME_MAP = {
        's3'    : 'AWS',
        'http'  : 'HTTP',
        'https' : 'HTTP',
        'ftp'   : 'HTTP',
        'gs'    : 'GCP',
        ''      : 'LOCAL',
    }

    def __init__(self, settings):
        assert isinstance(settings, dvc.settings.Settings) 

        #To handle ConfigI case
        if not hasattr(settings.config, '_config'):
            self._settings = settings
            self._cloud = DataCloudBase(None)
            return

        self._settings = settings
        self._config = self._settings.config._config

        cloud_type = self._config['Global'].get('Cloud', '').strip().upper()
        if cloud_type not in self.CLOUD_MAP.keys():
            raise ConfigError('Wrong cloud type %s specified' % cloud_type)

        if cloud_type not in self._config.keys():
            raise ConfigError('Can\'t find cloud section \'[%s]\' in config' % cloud_type)

        cloud_settings = self.get_cloud_settings(self._config, cloud_type, self._settings.path_factory)

        self.typ = cloud_type
        self._cloud = self.CLOUD_MAP[cloud_type](cloud_settings)

        self.sanity_check()

    @staticmethod
    def get_cloud_settings(config, cloud_type, path_factory):
        if cloud_type not in config.keys():
            cloud_config = None
        else:
            cloud_config = config[cloud_type]
        global_storage_path = config['Global'].get('StoragePath', None)
        cloud_settings = CloudSettings(path_factory, global_storage_path, cloud_config)
        return cloud_settings

    def sanity_check(self):
        """ sanity check a config

        check that we have a cloud and storagePath
        if aws, check can read credentials
        if google, check ProjectName

        Returns:
            (T,) if good
            (F, issues) if bad
        """
        key = 'Cloud'
        if key.lower() not in self._config['Global'].keys() or len(self._config['Global'][key]) < 1:
            raise ConfigError('Please set %s in section Global in config file %s' % (key, self.file))

        # now that a cloud is chosen, can check StoragePath
        sp = self._cloud.storage_path
        if sp is None or len(sp) == 0:
            raise ConfigError('Please set StoragePath = bucket/{optional path} in conf file "%s" '
                           'either in Global or a cloud specific section' % self.CONFIG)

        self._cloud.sanity_check()

    def _collect_dir(self, d):
        targets = []

        for root, dirs, files in os.walk(d):
            for f in files:
                path = os.path.join(root, f)
                item = self._settings.path_factory.data_item(path)
                targets.append(item)

        return targets

    def _collect_target(self, target):
        if System.islink(target):
            item = self._settings.path_factory.data_item(target)
            return [item]
        elif os.path.isdir(target):
            return self._collect_dir(target)

        Logger.warn('Target "{}" does not exist'.format(target))

        return []

    def _collect_targets(self, targets):
        collected = []

        for t in targets:
            collected += self._collect_target(t)

        return collected

    def _map_targets(self, f, targets, jobs):
        collected = self._collect_targets(targets)

        return map_progress(f, collected, jobs)

    def _import(self, target):
        url, item = target
        o = urlparse(url)

        typ = self.SCHEME_MAP.get(o.scheme, None)
        if typ == None:
            Logger.error('Not supported scheme \'{}\''.format(o.scheme))
            return None

        #To handle ConfigI case
        if not hasattr(self._settings.config, '_config'):
            self._config = None
            cloud_settings = None
        else:
            self._config = self._settings.config._config
            cloud_settings = self.get_cloud_settings(self._config, typ, self._settings.path_factory)

        cloud = self.CLOUD_MAP[typ](cloud_settings)

        return cloud.import_data(url, item)

    def sync(self, targets, jobs=1):
        return self._map_targets(self._cloud.sync, targets, jobs)

    def push(self, targets, jobs=1):
        return self._map_targets(self._cloud.push, targets, jobs)

    def pull(self, targets, jobs=1):
        return self._map_targets(self._cloud.pull, targets, jobs)

    def import_data(self, targets, jobs=1):
        return map_progress(self._import, targets, jobs)

    def remove(self, item):
        return self._cloud.remove(item)

    def status(self, targets, jobs=1):
        return self._map_targets(self._cloud.status, targets, jobs)

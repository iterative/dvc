"""
Data cloud(Local, AWS, GCP) drivers.
"""
import base64
import hashlib
import os
import threading
import requests
import filecmp
import math

from boto.s3.connection import S3Connection
try:
    import httplib
except ImportError:
    # Python3 workaround for ResumableDownloadHandler.
    # See https://github.com/boto/boto/pull/3755.
    import sys
    import http.client as httplib
    sys.modules['httplib'] = httplib
from boto.s3.resumable_download_handler import ResumableDownloadHandler
from google.cloud import storage as gc

try:
    from urlparse import urlparse
except ImportError:
    # Python 3
    # pylint: disable=no-name-in-module, import-error
    from urllib.parse import urlparse

import dvc
from dvc.cloud.instance_manager import CloudSettings
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.config import ConfigError
from dvc.progress import progress
from dvc.utils import copyfile
from dvc.cloud.credentials_aws import AWSCredentials
from dvc.system import System
from dvc.utils import map_progress

STATUS_UNKNOWN = 0
STATUS_OK = 1
STATUS_MODIFIED = 2
STATUS_NEW = 3
STATUS_DELETED = 4

STATUS_MAP = {
    # (local_exists, remote_exists, cmp)
    (True, True, True)  : STATUS_OK,
    (True, True, False) : STATUS_MODIFIED,
    (True, False, None) : STATUS_NEW,
    (False, True, None) : STATUS_DELETED,
}

class DataCloudError(DvcException):
    """ Data Cloud exception """
    def __init__(self, msg):
        super(DataCloudError, self).__init__('Data sync error: {}'.format(msg))


def sizeof_fmt(num, suffix='B'):
    """ Convert number of bytes to human-readable string """
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)


def percent_cb(name, part_complete, part_total, offset=0, multipart_total=None):
    """ Callback for updating target progress """
    complete = offset + part_complete
    total = multipart_total if multipart_total != None else part_total

    Logger.debug('{}: {} transferred out of {}'.format(name,
                                                       sizeof_fmt(complete),
                                                       sizeof_fmt(total)))
    progress.update_target(os.path.basename(name), complete, total)


def create_cb(name, offset=0, multipart_total=None):
    """ Create callback function for multipart object """
    return (lambda cur, tot: percent_cb(name, cur, tot, offset, multipart_total))


def file_md5(fname):
    """ get the (md5 hexdigest, md5 digest) of a file """
    if os.path.exists(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as fobj:
            for chunk in iter(lambda: fobj.read(1024*1000), b""):
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
        """
        Split storage path into parts. I.e. 'dvc-test/myrepo' -> ['dvc', 'myrepo']
        """
        return self.storage_path.strip('/').split('/', 1)

    @property
    def storage_bucket(self):
        """ Data -> StoragePath takes precedence; if doesn't exist, use cloud-specific """
        return self._storage_path_parts()[0]

    @property
    def storage_prefix(self):
        """
        Prefix within the bucket. I.e. 'myrepo' in 'dvc-test/myrepo'.
        """
        parts = self._storage_path_parts()
        if len(parts) > 1:
            return parts[1]
        return ''

    def cache_file_key(self, fname):
        """ Key of a file within the bucket """
        return '{}/{}'.format(self.storage_prefix, os.path.basename(fname)).strip('/')

    @staticmethod
    def tmp_file(fname):
        """ Temporary name for a partial download """
        return fname + '.part'

    def sanity_check(self):
        """
        Cloud-specific method to check config for basic requirements.
        """
        pass

    def _import(self, bucket, fin, fout):
        """
        Cloud-specific method for importing data file.
        """
        pass

    def push(self, path):
        """ Cloud-specific method for pushing data """
        pass

    def pull(self, path):
        """ Generic method for pulling data from the cloud """
        key_name = self.cache_file_key(path)
        return self._import(self.storage_bucket, key_name, path)

    def remove(self, path):
        """
        Cloud-specific method for removing data item from the cloud.
        """
        pass

    def _status(self, path):
        """
        Cloud-specific method for checking data item status.
        """
        pass

    def status(self, path):
        """
        Generic method for checking data item status.
        """
        return STATUS_MAP.get(self._status(path), STATUS_UNKNOWN)


class DataCloudLOCAL(DataCloudBase):
    """
    Driver for local storage.
    """
    def push(self, path):
        Logger.debug('sync to cloud ' + path + " " + self.storage_path)
        copyfile(path, self.storage_path)
        return path

    def _import(self, bucket, i, path):
        inp = os.path.join(self.storage_path, i)
        tmp_file = self.tmp_file(path)
        try:
            copyfile(inp, tmp_file)
        except Exception as exc:
            Logger.error('Failed to copy "{}": {}'.format(i, exc))
            return None

        os.rename(tmp_file, path)

        return path

    def pull(self, path):
        Logger.debug('sync from cloud ' + path)
        return self._import(None, path, path)

    def remove(self, path):
        Logger.debug('rm from cloud ' + path)
        os.remove(path)

    def import_data(self, path, out):
        Logger.debug('import from cloud ' + path + " " + out)
        return self._import(None, path, out)

    def _status(self, path):
        local = path
        remote = '{}/{}'.format(self.storage_path, os.path.basename(local))

        remote_exists = os.path.exists(remote)
        local_exists = os.path.exists(local)
        diff = None
        if local_exists and remote_exists:
            diff = filecmp.cmp(local, remote)

        return (local_exists, remote_exists, diff)

class DataCloudHTTP(DataCloudBase):
    """
    Driver for http cloud.
    """
    def push(self, item):
        raise Exception('Not implemented yet')

    def pull(self, item):
        raise Exception('Not implemented yet')

    def remove(self, item):
        raise Exception('Not implemented yet')

    def status(self, item):
        raise Exception('Not implemented yet')

    @staticmethod
    def _downloaded_size(fname):
        """
        Check how much is already downloaded.
        """
        if os.path.exists(fname):
            downloaded = os.path.getsize(fname)
            header = {'Range': 'bytes=%d-' % downloaded}

            Logger.debug('found existing {} file, resuming download'.format(fname))

            return (downloaded, header)

        return (0, None)

    @staticmethod
    def _get_header(req, name):
        """
        Get header value from request.
        """
        val = req.headers.get(name)
        if val == None:
            Logger.debug('\'{}\' not supported by the server'.format(name))

        return val

    def _verify_downloaded_size(self, req, downloaded_size):
        """
        Check that server supports resuming downloads.
        """
        content_range = self._get_header(req, 'content-range')
        if downloaded_size and content_range == None:
            Logger.debug('Can\'t resume download')
            return 0

        return downloaded_size

    def _download(self, req, fname, downloaded):
        """
        Download file with progress bar.
        """
        mode = 'ab' if downloaded else 'wb'
        name = os.path.basename(req.url)
        total_length = self._get_header(req, 'content-length')
        chunk_size = 1024 * 100

        progress.update_target(name, downloaded, total_length)

        with open(fname, mode) as fobj:
            for chunk in req.iter_content(chunk_size=chunk_size):
                if not chunk:  # filter out keep-alive new chunks
                    continue

                fobj.write(chunk)
                downloaded += len(chunk)
                progress.update_target(name, downloaded, total_length)


        progress.finish_target(name)

    def _verify_md5(self, req, fname):
        """
        Verify md5 of a downloaded file if server supports 'content-md5' header.
        """
        md5 = file_md5(fname)[0]
        content_md5 = self._get_header(req, 'content-md5')

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

        tmp_file = self.tmp_file(item.data.dvc)

        downloaded, header = self._downloaded_size(tmp_file)
        req = requests.get(url, stream=True, headers=header)
        downloaded = self._verify_downloaded_size(req, downloaded)

        try:
            self._download(req, tmp_file, downloaded)
        except Exception as exc:
            Logger.error('Failed to download "{}": {}'.format(url, exc))
            return None

        if not self._verify_md5(req, tmp_file):
            return None

        os.rename(tmp_file, item.data.dvc)
        item.move_data_to_cache()

        return item


class DataCloudAWS(DataCloudBase):
    """ DataCloud class for Amazon Web Services """
    def __init__(self, cloud_settings):
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
        """
        Try obtaining path to aws credentials from config file.
        """
        paths = []
        credpath = self._cloud_settings.cloud_config.get('CredentialPath', None)
        if credpath is not None and len(credpath) > 0:
            credpath = os.path.expanduser(credpath)
            if os.path.isfile(credpath):
                paths.append(credpath)
            else:
                Logger.warn('AWS CredentialPath "%s" not found;'
                            'falling back to default "%s"' % (credpath, default))
                paths.append(default)
        else:
            paths.append(default)
        return paths

    def _get_bucket_aws(self, bucket_name):
        """ get a bucket object, aws """
        if all([self._aws_creds.access_key_id,
                self._aws_creds.secret_access_key,
                self.aws_region_host]):
            conn = S3Connection(self._aws_creds.access_key_id,
                                self._aws_creds.secret_access_key,
                                host=self.aws_region_host)
        else:
            conn = S3Connection()
        bucket = conn.lookup(bucket_name)
        if bucket is None:
            raise DataCloudError('Storage path {} is not setup correctly'.format(bucket_name))
        return bucket

    @staticmethod
    def _cmp_checksum(key, fname):
        """
        Verify local and remote checksums. Used 'dvc-md5' metadata if supported
        or falls back to etag.
        """
        md5_cloud = key.metadata.get('dvc-md5', None)
        md5_local = file_md5(fname)[0]

        if md5_cloud == None:
            md5_cloud = key.etag[1:-1]

        if md5_cloud == md5_local:
            return True

        return False

    @staticmethod
    def _upload_tracker(fname):
        """
        File name for upload tracker.
        """
        return fname + '.upload'

    @staticmethod
    def _download_tracker(fname):
        """
        File name for download tracker.
        """
        return fname + '.download'

    def _import(self, bucket_name, key_name, fname):

        bucket = self._get_bucket_aws(bucket_name)

        tmp_file = self.tmp_file(fname)
        name = os.path.basename(fname)
        key = bucket.get_key(key_name)
        if not key:
            Logger.error('File "{}" does not exist in the cloud'.format(key_name))
            return None

        if self._cmp_checksum(key, fname):
            Logger.debug('File "{}" matches with "{}".'.format(fname, key_name))
            return fname

        Logger.debug('Downloading cache file from S3 "{}/{}" to "{}"'.format(bucket.name,
                                                                             key_name,
                                                                             fname))

        res_h = ResumableDownloadHandler(tracker_file_name=self._download_tracker(tmp_file),
                                         num_retries=10)
        try:
            key.get_contents_to_filename(tmp_file, cb=create_cb(name), res_download_handler=res_h)
        except Exception as exc:
            Logger.error('Failed to download "{}": {}'.format(key_name, exc))
            return None

        os.rename(tmp_file, fname)

        progress.finish_target(name)
        Logger.debug('Downloading completed')

        return fname

    def _write_upload_tracker(self, fname, mp_id):
        """
        Write multipart id to upload tracker.
        """
        try:
            open(self._upload_tracker(fname), 'w+').write(mp_id)
        except Exception as exc:
            Logger.debug("Failed to write upload tracker file for {}: {}".format(fname, exc))

    def _unlink_upload_tracker(self, fname):
        """
        Remove upload tracker file.
        """
        try:
            os.unlink(self._upload_tracker(fname))
        except Exception as exc:
            Logger.debug("Failed to unlink upload tracker file for {}: {}".format(fname, exc))

    def _resume_multipart(self, key, fname):
        """
        Try resuming multipart upload.
        """
        try:
            mp_id = open(self._upload_tracker(fname), 'r').read()
        except Exception as exc:
            Logger.debug("Failed to read upload tracker file for {}: {}".format(fname, exc))
            return None

        for part in key.bucket.get_all_multipart_uploads():
            if part.id != mp_id:
                continue

            Logger.debug("Found existing multipart {}".format(mp_id))
            return part

        return None

    def _create_multipart(self, key, fname):
        """
        Create multipart upload and save info to tracker file.
        """
        # AWS doesn't provide easilly accessible md5 for multipart
        # objects, so we have to store our own md5 sum to use later.
        metadata = {'dvc-md5' : str(file_md5(fname)[0])}
        multipart = key.bucket.initiate_multipart_upload(key.name, metadata=metadata)
        self._write_upload_tracker(fname, multipart.id)
        return multipart

    def _get_multipart(self, key, fname):
        """
        Try resuming multipart upload if supported.
        """
        multipart = self._resume_multipart(key, fname)
        if multipart != None:
            return multipart

        return self._create_multipart(key, fname)

    @staticmethod
    def _skip_part(multipart, part_num, size):
        """
        Skip part of multipart upload if it has been already uploaded to the server.
        """
        for part in multipart.get_all_parts():
            if part.part_number == part_num and part.size == size:# and p.etag and p.last_modified
                Logger.debug("Skipping part #{}".format(str(part_num)))
                return True
        return False

    def _push_multipart(self, key, fname):
        """
        Upload local file to cloud as a multipart upload.
        """
        multipart = self._get_multipart(key, fname)

        source_size = os.stat(fname).st_size
        chunk_size = 50*1024*1024
        chunk_count = int(math.ceil(source_size / float(chunk_size)))

        with open(fname, 'rb') as fobj:
            for i in range(chunk_count):
                offset = i * chunk_size
                left = source_size - offset
                size = min([chunk_size, left])
                part_num = i + 1

                if self._skip_part(multipart, part_num, size):
                    continue

                fobj.seek(offset)
                multipart.upload_part_from_file(fp=fobj,
                                                replace=False,
                                                size=size,
                                                num_cb=100,
                                                part_num=part_num,
                                                cb=create_cb(fname, offset, source_size))

        if len(multipart.get_all_parts()) != chunk_count:
            raise Exception("Couldn't upload all file parts")

        multipart.complete_upload()
        self._unlink_upload_tracker(fname)

    def push(self, path):
        """ push, aws version """

        aws_key = self.cache_file_key(path)
        bucket = self._get_bucket_aws(self.storage_bucket)
        key = bucket.get_key(aws_key)
        if key:
            Logger.debug('File already uploaded to the cloud. Checksum validation...')

            if self._cmp_checksum(key, path):
                Logger.debug('File checksum matches. No uploading is needed.')
                return path

            Logger.debug('Checksum miss-match. Re-uploading is required.')

        key = bucket.new_key(aws_key)

        try:
            self._push_multipart(key, path)
        except Exception as exc:
            Logger.error('Failed to upload "{}": {}'.format(path, exc))
            return None

        progress.finish_target(os.path.basename(path))

        return path

    def _status(self, path):
        aws_key = self.cache_file_key(path)
        bucket = self._get_bucket_aws(self.storage_bucket)
        key = bucket.get_key(aws_key)

        remote_exists = key is not None
        local_exists = os.path.exists(path)
        diff = None
        if remote_exists and local_exists:
            diff = self._cmp_checksum(key, path)

        return (local_exists, remote_exists, diff)

    def remove(self, path):
        aws_file_name = self.cache_file_key(path)

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
                Logger.warn('[Cmd-Remove] S3 remove warning: '
                            'file "{}" does not exist in S3'.format(aws_file_name))
            else:
                key.delete()
                Logger.info('[Cmd-Remove] File "{}" was removed from S3'.format(aws_file_name))


class DataCloudGCP(DataCloudBase):
    """ DataCloud class for Google Cloud Platform """
    @property
    def gc_project_name(self):
        """
        Get project name from config.
        """
        return self._cloud_settings.cloud_config.get('ProjectName', None)

    def sanity_check(self):
        project = self.gc_project_name
        if project is None or len(project) < 1:
            raise ConfigError('can\'t read google cloud project name. '
                              'Please set ProjectName in section GC.')

    def _get_bucket_gc(self, storage_bucket):
        """ get a bucket object, gc """
        client = gc.Client(project=self.gc_project_name)
        bucket = client.bucket(storage_bucket)
        if not bucket.exists():
            raise DataCloudError('sync up: google cloud bucket {} '
                                 'doesn\'t exist'.format(self.storage_bucket))
        return bucket

    @staticmethod
    def _cmp_checksum(blob, fname):
        """
        Verify local and remote checksums.
        """
        md5 = file_md5(fname)[1]
        b64_encoded_md5 = base64.b64encode(md5).decode() if md5 else None

        if blob.md5_hash == b64_encoded_md5:
            return True

        return False

    def _import(self, bucket_name, key, fname):

        bucket = self._get_bucket_gc(bucket_name)

        name = os.path.basename(fname)
        tmp_file = self.tmp_file(fname)

        blob = bucket.get_blob(key)
        if not blob:
            Logger.error('File "{}" does not exist in the cloud'.format(key))
            return None

        if self._cmp_checksum(blob, fname):
            Logger.debug('File "{}" matches with "{}".'.format(fname, key))
            return fname

        Logger.debug('Downloading cache file from gc "{}/{}"'.format(bucket.name, key))

        # percent_cb is not available for download_to_filename, so
        # lets at least update progress at keypoints(start, finish)
        progress.update_target(name, 0, None)

        try:
            blob.download_to_filename(tmp_file)
        except Exception as exc:
            Logger.error('Failed to download "{}": {}'.format(key, exc))
            return None

        os.rename(tmp_file, fname)

        progress.finish_target(name)

        Logger.debug('Downloading completed')

        return fname

    def push(self, path):
        """ push, gcp version """

        bucket = self._get_bucket_gc(self.storage_bucket)
        blob_name = self.cache_file_key(path)
        name = os.path.basename(path)

        blob = bucket.get_blob(blob_name)
        if blob is not None and blob.exists():
            if self._cmp_checksum(blob, path):
                Logger.debug('checksum %s matches.  Skipping upload' % path)
                return path
            Logger.debug('checksum %s mismatch.  re-uploading' % path)

        # same as in _import
        progress.update_target(name, 0, None)

        blob = bucket.blob(blob_name)
        blob.upload_from_filename(path)

        progress.finish_target(name)
        Logger.debug('uploading %s completed' % path)

        return path

    def _status(self, path):
        """ status, gcp version """

        bucket = self._get_bucket_gc(self.storage_bucket)
        blob_name = self.cache_file_key(path)
        blob = bucket.get_blob(blob_name)

        remote_exists = blob is not None and blob.exists()
        local_exists = os.path.exists(path)
        diff = None
        if remote_exists and local_exists:
            diff = self._cmp_checksum(blob, path)

        return (local_exists, remote_exists, diff)

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

    def __init__(self, config):
        self._config = config

        cloud_type = self._config['Global'].get('Cloud', '').strip().upper()
        if cloud_type not in self.CLOUD_MAP.keys():
            raise ConfigError('Wrong cloud type %s specified' % cloud_type)

        if cloud_type not in self._config.keys():
            raise ConfigError('Can\'t find cloud section \'[%s]\' in config' % cloud_type)

        cloud_settings = self.get_cloud_settings(self._config,
                                                 cloud_type)

        self.typ = cloud_type
        self._cloud = self.CLOUD_MAP[cloud_type](cloud_settings)

        self.sanity_check()

    @staticmethod
    def get_cloud_settings(config, cloud_type):
        """
        Obtain cloud settings from config.
        """
        if cloud_type not in config.keys():
            cloud_config = None
        else:
            cloud_config = config[cloud_type]
        global_storage_path = config['Global'].get('StoragePath', None)
        cloud_settings = CloudSettings(global_storage_path, cloud_config)
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
        if key.lower() not in [k.lower() for k in self._config['Global'].keys()] or len(self._config['Global'][key]) < 1:
            raise ConfigError('Please set %s in section Global in config file' % key)

        # now that a cloud is chosen, can check StoragePath
        storage_path = self._cloud.storage_path
        if storage_path is None or len(storage_path) == 0:
            raise ConfigError('Please set StoragePath = bucket/{optional path} '
                              'in config file in a cloud specific section')

        self._cloud.sanity_check()

    def _map_targets(self, func, targets, jobs):
        """
        Process targets as data items in parallel.
        """
        return map_progress(func, targets, jobs)

    def sync(self, targets, jobs=1):
        """
        Sync data items in a cloud-agnostic way.
        """
        return self._map_targets(self._cloud.sync, targets, jobs)

    def push(self, targets, jobs=1):
        """
        Push data items in a cloud-agnostic way.
        """
        return self._map_targets(self._cloud.push, targets, jobs)

    def pull(self, targets, jobs=1):
        """
        Pull data items in a cloud-agnostic way.
        """
        return self._map_targets(self._cloud.pull, targets, jobs)

    def status(self, targets, jobs=1):
        """
        Check status of data items in a cloud-agnostic way.
        """
        return self._map_targets(self._cloud.status, targets, jobs)

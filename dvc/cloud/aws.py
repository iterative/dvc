import os
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

from dvc.logger import Logger
from dvc.progress import progress
from dvc.cloud.credentials_aws import AWSCredentials
from dvc.cloud.base import DataCloudError, DataCloudBase
from dvc.utils import file_md5


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

"""Helpers for other modules."""

from __future__ import unicode_literals

from dvc.utils.collections import dict_filter
from dvc.utils.compat import str, open

import os
import json
import hashlib
import logging
import schema


logger = logging.getLogger(__name__)

LOCAL_CHUNK_SIZE = 1024 * 1024
LARGE_FILE_SIZE = 1024 * 1024 * 1024

CHECKSUM_MD5 = "md5"
CHECKSUM_SHA256 = "sha256"
CHECKSUM_MAP = {CHECKSUM_MD5: hashlib.md5, CHECKSUM_SHA256: hashlib.sha256}


CHECKSUM_LOCAL_SCHEMA = {
    schema.Optional(CHECKSUM_MD5): schema.Or(str, None),
    schema.Optional(CHECKSUM_SHA256): schema.Or(str, None),
}

# NOTE: currently there are only 4 possible checksum names:
#
#    1) md5 (LOCAL, SSH, GS);
#    2) etag (S3);
#    3) checksum (HDFS);
#    4) sha256 (LOCAL);
#
# so when a few types of outputs share the same name, we only need
# specify it once.
CHECKSUM_ETAG = "etag"
CHECKSUM_CHECKSUM = "checksum"

CHECKSUM_SCHEMA = CHECKSUM_LOCAL_SCHEMA.copy()
CHECKSUM_SCHEMA[schema.Optional(CHECKSUM_ETAG)] = schema.Or(str, None)
CHECKSUM_SCHEMA[schema.Optional(CHECKSUM_CHECKSUM)] = schema.Or(str, None)

LOCAL_SUPPORTED_CHECKSUM_TYPES = [CHECKSUM_MD5, CHECKSUM_SHA256]
HTTP_SUPPORTED_CHECKSUM_TYPES = [CHECKSUM_ETAG, CHECKSUM_MD5]
SSH_SUPPORTED_CHECKSUM_TYPES = [CHECKSUM_MD5]
AZURE_SUPPORTED_CHECKSUM_TYPES = [CHECKSUM_ETAG]
GS_SUPPORTED_CHECKSUM_TYPES = [CHECKSUM_MD5]
HDFS_SUPPORTED_CHECKSUM_TYPES = [CHECKSUM_CHECKSUM]
S3_SUPPORTED_CHECKSUM_TYPES = [CHECKSUM_ETAG]


def checksum_types_from_str(checksum_types, supported_types):
    if not supported_types:
        return None
    if isinstance(checksum_types, str):
        checksum_types = [h.strip().lower() for h in checksum_types.split(",")]
    if not isinstance(checksum_types, list) or len(checksum_types) < 1:
        return None
    for h in checksum_types:
        if h not in supported_types:
            return None
    return checksum_types


def dos2unix(data):
    return data.replace(b"\r\n", b"\n")


def file_checksum(fname, checksum_type=CHECKSUM_MD5):
    """ get the (md5 hexdigest, md5 digest) of a file """
    from dvc.progress import progress
    from dvc.istextfile import istextfile

    if os.path.exists(fname):
        hasher = CHECKSUM_MAP[checksum_type]()
        binary = not istextfile(fname)
        size = os.path.getsize(fname)
        bar = False
        if size >= LARGE_FILE_SIZE:
            bar = True
            msg = "Computing md5 for a large file {}. This is only done once."
            logger.info(msg.format(os.path.relpath(fname)))
            name = os.path.relpath(fname)
            total = 0

        with open(fname, "rb") as fobj:
            while True:
                data = fobj.read(LOCAL_CHUNK_SIZE)
                if not data:
                    break

                if bar:
                    total += len(data)
                    progress.update_target(name, total, size)

                if binary:
                    chunk = data
                else:
                    chunk = dos2unix(data)

                hasher.update(chunk)

        if bar:
            progress.finish_target(name)

        return (hasher.hexdigest(), hasher.digest())
    else:
        return (None, None)


def bytes_checksum(byts, checksum_type=CHECKSUM_MD5):
    hasher = CHECKSUM_MAP[checksum_type]()
    hasher.update(byts)
    return hasher.hexdigest()


def dict_checksum(d, exclude=None, checksum_type=CHECKSUM_MD5):
    filtered = dict_filter(d, exclude)
    byts = json.dumps(filtered, sort_keys=True).encode("utf-8")
    return bytes_checksum(byts, checksum_type)

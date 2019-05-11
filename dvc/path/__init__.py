from dvc.utils.compat import urlunsplit


class Schemes:
    SSH = "ssh"
    HDFS = "hdfs"
    S3 = "s3"
    AZURE = "azure"
    HTTP = "http"
    GS = "gs"
    LOCAL = "local"
    OSS = "oss"


class BasePathInfo(object):
    scheme = None

    def __init__(self, url=None, path=None):
        self.url = url
        self.path = path

    def __str__(self):
        return self.url


class DefaultCloudPathInfo(BasePathInfo):
    def __init__(self, bucket, url=None, path=None):
        super(DefaultCloudPathInfo, self).__init__(url, path)
        self.bucket = bucket

    def __str__(self):
        if not self.url:
            return urlunsplit((self.scheme, self.bucket, self.path, "", ""))
        return self.url

from dvc.path import Schemes, DefaultCloudPathInfo


class S3PathInfo(DefaultCloudPathInfo):
    scheme = Schemes.S3

from dvc.path import Schemes, DefaultCloudPathInfo


class OSSPathInfo(DefaultCloudPathInfo):
    scheme = Schemes.OSS

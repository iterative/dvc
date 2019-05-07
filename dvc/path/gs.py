from dvc.path import Schemes, DefaultCloudPathInfo


class GSPathInfo(DefaultCloudPathInfo):
    scheme = Schemes.GS

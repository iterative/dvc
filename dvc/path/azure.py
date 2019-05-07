from dvc.path import Schemes, DefaultCloudPathInfo


class AzurePathInfo(DefaultCloudPathInfo):
    scheme = Schemes.AZURE

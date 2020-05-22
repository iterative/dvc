from dvc.output.base import BaseOutput
from dvc.remote.azure import AzureRemote


class AzureOutput(BaseOutput):
    REMOTE = AzureRemote

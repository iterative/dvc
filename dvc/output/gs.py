from dvc.output.s3 import OutputS3
from dvc.remote.gs import RemoteGS


class OutputGS(OutputS3):
    REMOTE = RemoteGS

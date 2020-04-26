from dvc.output.s3 import OutputS3
from dvc.remote.gs import GSRemote


class OutputGS(OutputS3):
    REMOTE = GSRemote

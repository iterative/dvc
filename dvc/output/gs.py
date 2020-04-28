from dvc.output.s3 import S3Output
from dvc.remote.gs import GSRemote


class GSOutput(S3Output):
    REMOTE = GSRemote

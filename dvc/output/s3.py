from dvc.output.base import BaseOutput

from ..fs.s3 import S3FileSystem


class S3Output(BaseOutput):
    FS_CLS = S3FileSystem

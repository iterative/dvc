from dvc.dependency.s3 import DependencyS3


class OutputS3(DependencyS3):
    def __init__(self, stage, path, etag=None):
        raise NotImplemented

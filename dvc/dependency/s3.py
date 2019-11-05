from __future__ import unicode_literals

from dvc.dependency.base import DependencyBase
from dvc.output.s3 import OutputS3


class DependencyS3(DependencyBase, OutputS3):
    pass

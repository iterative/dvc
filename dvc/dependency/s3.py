from __future__ import unicode_literals

from dvc.output.s3 import OutputS3
from dvc.dependency.base import DependencyBase


class DependencyS3(DependencyBase, OutputS3):
    pass

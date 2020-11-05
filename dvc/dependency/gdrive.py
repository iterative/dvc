from dvc.dependency.base import BaseDependency
from dvc.output.gdrive import GDriveOutput


class GDriveDependency(BaseDependency, GDriveOutput):
    pass

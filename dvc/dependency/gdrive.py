from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput
from dvc.remote.gdrive import GDriveRemote


class GDriveDependency(BaseDependency, BaseOutput):
    REMOTE = GDriveRemote

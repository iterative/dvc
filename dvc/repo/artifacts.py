import os
from typing import TYPE_CHECKING

from dvc.annotations import Artifact
from dvc.dvcfile import FileMixin

if TYPE_CHECKING:
    from dvc.repo import Repo


class ArtifactsFile(FileMixin):
    from dvc.schema import ARTIFACTS_SCHEMA as SCHEMA


class Artifacts:
    repo: "Repo"

    def __init__(self, repo) -> None:
        self.repo = repo

    def _read(self):
        # merge artifacts from all dvc.yaml files found
        artifacts = {}
        for dvcfile, dvcfile_artifacts in self.repo.index._artifacts.items():
            # read the artifacts.yaml file if needed
            if isinstance(dvcfile_artifacts, str):
                dvcfile_artifacts = ArtifactsFile(
                    self.repo,
                    os.path.join(os.path.dirname(dvcfile), dvcfile_artifacts),
                    verify=False,
                ).load()
            for name, value in dvcfile_artifacts.items():
                if name in artifacts:
                    # Q: maybe better to issue a warning here and take the first one?
                    raise ValueError(
                        f"Duplicated artifact ID: {name} in {dvcfile} and {artifacts[name]['dvcfile']}"
                    )
                artifacts[name] = {"dvcfile": dvcfile, "annotation": Artifact(**value)}
        return artifacts

    def read(self):
        return {name: value["annotation"] for name, value in self._read().items()}

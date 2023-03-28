import os
from typing import TYPE_CHECKING, Any, Dict

from dvc.annotations import Artifact
from dvc.dvcfile import FileMixin
from dvc.exceptions import DuplicatedArtifactError

if TYPE_CHECKING:
    from dvc.repo import Repo


class ArtifactsFile(FileMixin):
    from dvc.schema import ARTIFACTS_SCHEMA as SCHEMA

    def dump(self, stage, **kwargs):
        raise NotImplementedError

    def merge(self, ancestor, other, allowed=None):
        raise NotImplementedError


class Artifacts:
    repo: "Repo"

    def __init__(self, repo) -> None:
        self.repo = repo

    def _read(self):
        # merge artifacts from all dvc.yaml files found
        artifacts: Dict[str, Dict[str, Any]] = {}
        for (
            dvcfile,
            dvcfile_artifacts,
        ) in self.repo.index._artifacts.items():  # pylint: disable=protected-access
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
                    raise DuplicatedArtifactError(
                        name, dvcfile, artifacts[name]["dvcfile"]
                    )
                artifacts[name] = {"dvcfile": dvcfile, "annotation": Artifact(**value)}
        return artifacts

    def read(self):
        return {name: value["annotation"] for name, value in self._read().items()}

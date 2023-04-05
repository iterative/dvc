import logging
import os
import re
from typing import TYPE_CHECKING, Dict

from dvc.annotations import Artifact
from dvc.dvcfile import FileMixin
from dvc.utils import relpath

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


NAME_RE = re.compile(r"^[a-z]([a-z0-9-]*[a-z0-9])?$")


def name_is_compatible(name: str) -> bool:
    return bool(NAME_RE.search(name))


def check_name_format(name: str) -> None:
    if not name_is_compatible(name):
        logger.warning(
            "Can't use '%s' as artifact name (ID)."
            " You can use letters and numbers, and use '-' as separator"
            " (but not at the start or end). The first character must be a letter.",
            name,
        )


class ArtifactsFile(FileMixin):
    from dvc.schema import SINGLE_ARTIFACT_SCHEMA as SCHEMA

    def dump(self, stage, **kwargs):
        raise NotImplementedError

    def merge(self, ancestor, other, allowed=None):
        raise NotImplementedError


class Artifacts:
    def __init__(self, repo: "Repo") -> None:
        self.repo = repo

    def read(self) -> Dict[str, Dict[str, Artifact]]:
        artifacts: Dict[str, Dict[str, Artifact]] = {}
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
            if not dvcfile_artifacts:
                continue
            dvcyaml = relpath(dvcfile, self.repo.root_dir)
            artifacts[dvcyaml] = {}
            for name, value in dvcfile_artifacts.items():
                check_name_format(name)
                artifacts[dvcyaml][name] = Artifact(**value)
        return artifacts

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


NAME_RE = re.compile("^[a-z][a-z0-9-/.]*[a-z0-9]$")


def check_name_format(name: str) -> None:
    # "/" forbidden, only in dvc exp as we didn't support it for now.
    if not bool(NAME_RE.search(name)):
        logger.warning(
            "To be compatible with Git tags and GTO, artifact name ('%s') "
            "must satisfy the following regex: %s",
            name,
            NAME_RE.pattern,
        )


class ArtifactsFile(FileMixin):
    from dvc.schema import ARTIFACTS_SCHEMA as SCHEMA

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
                artifacts[dvcyaml][name] = Artifact(**{"path": name, **(value or {})})
        return artifacts

import logging
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional

from dvc.annotations import Artifact
from dvc.dvcfile import FileMixin, ProjectFile
from dvc.utils import relpath
from dvc.utils.serialize import modify_yaml

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


# Constants are taken from GTO.
# When we make it a dependency, we can import them instead
SEPARATOR_IN_NAME = ":"
DIRNAME = r"[a-z0-9-_./]+"
NAME = r"[a-z0-9]([a-z0-9-/]*[a-z0-9])?"
NAME_RE = re.compile(f"^{NAME}$")
FULLNAME = f"((?P<dirname>{DIRNAME}){SEPARATOR_IN_NAME})?(?P<name>{NAME})"
FULLNAME_RE = re.compile(f"^{FULLNAME}$")


def name_is_compatible(name: str) -> bool:
    return bool(NAME_RE.search(name))


def check_name_format(name: str) -> None:
    if not name_is_compatible(name):
        logger.warning(
            "Can't use '%s' as artifact name (ID)."
            " You can use letters and numbers, and use '-' as separator"
            " (but not at the start or end).",
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
            dvcyaml = relpath(dvcfile, self.repo.root_dir)
            artifacts[dvcyaml] = {}
            for name, value in dvcfile_artifacts.items():
                check_name_format(name)
                artifacts[dvcyaml][name] = Artifact(**value)
        return artifacts

    def add(self, name: str, artifact: Artifact, dvcfile: Optional[str] = None):
        # this doesn't update it "in place": self.read() won't return the updated value
        dvcfile = dvcfile or ""
        if not dvcfile.endswith(PROJECT_FILE):
            dvcfile = os.path.join(dvcfile or "", PROJECT_FILE)
        dvcfile_abspath = os.path.join(self.repo.root_dir, dvcfile)
        Path(os.path.dirname(dvcfile_abspath)).mkdir(exist_ok=True, parents=True)
        _update_project_file(
            ProjectFile(self.repo, dvcfile_abspath), name, artifact.to_dict()
        )
        return dvcfile_abspath


def _update_project_file(project_file, name, artifact):
    with modify_yaml(project_file.path, fs=project_file.repo.fs) as data:
        if not data:
            logger.info("Creating '%s'", project_file.relpath)

        data["artifacts"] = data.get("artifacts", {})
        data["artifacts"].update({name: artifact})

    project_file.repo.scm_context.track_file(project_file.relpath)

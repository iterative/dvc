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


SEPARATOR_IN_NAME = ":"
DIRNAME = r"[a-z0-9-_./]+"  # TODO: re-examine? notice is coupled with GTO
NAME = r"[a-z]([a-z0-9-/]*[a-z0-9])?"  # just like in GTO now w/o "/"
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
            dvcyaml = relpath(dvcfile, self.repo.root_dir)
            artifacts[dvcyaml] = {}
            for name, value in dvcfile_artifacts.items():
                check_name_format(name)
                artifacts[dvcyaml][name] = Artifact(**value)
        return artifacts

    def add(self, name: str, artifact: Artifact, dvcfile: Optional[str] = None):
        dvcfile, name = parse_name(name, dvcfile)
        # this doesn't update it "in place": self.read() won't return the updated value
        dvcfile_dir = os.path.join(self.repo.root_dir, dvcfile or "")
        Path(dvcfile_dir).mkdir(exist_ok=True)
        dvcyaml_path = os.path.join(dvcfile_dir, "dvc.yaml")
        _update_project_file(
            ProjectFile(self.repo, dvcyaml_path), name, artifact.to_dict()
        )
        return dvcyaml_path


def _update_project_file(project_file, name, artifact):
    with modify_yaml(project_file.path, fs=project_file.repo.fs) as data:
        if not data:
            logger.info("Creating '%s'", project_file.relpath)

        data["artifacts"] = data.get("artifacts", {})
        data["artifacts"].update({name: artifact})

    project_file.repo.scm_context.track_file(project_file.relpath)


def parse_name(name, dvcfile=None):
    if not dvcfile:
        return _parse_name(name)
    dvcfile_from_name, subname = _parse_name(name)
    if dvcfile_from_name != dvcfile:
        raise ValueError(f"Artifact name {name} doesn't match given dvcfile {dvcfile}")
    return dvcfile_from_name, subname


def _parse_name(name):
    match = FULLNAME_RE.search(name)
    if not match:
        raise ValueError(f"Invalid artifact name: {name}")
    subdir = match["dirname"] or ""
    subname = match.group("name")
    return subdir, subname

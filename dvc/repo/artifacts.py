import logging
import os
import re
from typing import TYPE_CHECKING, Dict, Optional

from dvc.annotations import Artifact
from dvc.dvcfile import FileMixin, ProjectFile
from dvc.utils import relpath

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


SEPARATOR_IN_NAME = ":"
dirname = r"[a-z0-9-_./]+"  # improve? but change in conjunction with GTO
name = r"[a-z]([a-z0-9-]*[a-z0-9])?"  # just like in GTO now w/o "/"
name_re = re.compile(f"^{name}$")
fullname = f"((?P<dirname>{dirname}){SEPARATOR_IN_NAME})?(?P<name>{name})"
fullname_re = re.compile(f"^{fullname}$")


def name_is_compatible(name: str) -> bool:
    return bool(name_re.search(name))


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

    def add(self, name: str, artifact: Artifact, dvcfile: Optional[str] = None):
        dvcfile, name = parse_name(name, dvcfile)
        # this doesn't update it "in place", so self.get() won't return the updated value
        # TODO: support writing in `artifacts: artifacts.yaml` case
        # TODO: check `dvcfile` exists - or maybe it's checked in `ProjectFile` already?
        dvcfile_dir = os.path.join(self.repo.root_dir, dvcfile)
        if not os.path.exists(dvcfile_dir):
            os.mkdir(dvcfile_dir)
        dvcyaml_path = os.path.join(dvcfile_dir, "dvc.yaml")
        ProjectFile(self.repo, dvcyaml_path)._dump_pipeline_file_artifacts(
            name, artifact.to_dict() if artifact else {}
        )
        return dvcyaml_path

    def remove(self, name: str, dvcfile: Optional[str] = None):
        pass
        # do the same as add, but pass None to `_dump_pipeline_file_artifacts` as artifact


def parse_name(fullname, dvcfile=None):
    if not dvcfile:
        return _parse_name(fullname)
    dvcfile_from_name, name = _parse_name(fullname)
    if dvcfile_from_name != dvcfile:
        raise ValueError(
            f"Artifact name {fullname} doesn't match given dvcfile {dvcfile}"
        )
    return dvcfile_from_name, name


def _parse_name(fullname):
    match = fullname_re.search(fullname)
    if not match:
        raise ValueError(f"Invalid artifact name: {fullname}")
    print(match, match["dirname"], match["name"])
    dirname = match["dirname"] or ""
    name = match.group("name")
    return dirname, name

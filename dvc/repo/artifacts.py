import os
from typing import TYPE_CHECKING

from dvc.annotations import Artifact
from dvc.dvcfile import FileMixin, ProjectFile

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
                    # maybe better to issue a warning here and take the first one?
                    # QUESTION: how to deal with the same artifact name in different dvc.yaml files?
                    raise ValueError(
                        f"Duplicated artifact ID: {name} in {dvcfile} and {artifacts[name]['dvcfile']}"
                    )
                artifacts[name] = {"dvcfile": dvcfile, "annotation": Artifact(**value)}
        return artifacts

    def read(self):
        return {name: value["annotation"] for name, value in self._read().items()}

    def get(self, name):
        return self.read().get(name, Artifact())

    # TODO: big question: how this will work for DVC subprojects?
    # i.e. how should we match Git tags with subprojects, if the same artifacts exists in both?
    # e.g. how Studio should figure out what Git tags belong to this subproject?
    # e.g. how you figure out what artifact to take in CI/CD?
    def _resolve_dvcfile_path(self, name=None, must_exist=False, prefered_path=None):
        artifacts = self._read()
        # if artifact with given name already exists, give the path to its file
        if name:
            if name in artifacts:
                return artifacts[name]["dvcfile"]
            if must_exist:
                raise ValueError(f"No artifact {name} found")
        # if `prefered_path` is given, use it
        # this will allow to write to dvclive's dvc.yaml
        # QUESTION 1: is it OK that dvclive.log_artifact() will update existing artifact that's in some "external" dvc.yaml?
        # QUESTION 2: if I have `artifacts:` section in the dvc.yaml that's "external" for dvclive, should I write to it?
        if prefered_path:
            return prefered_path
        # return the first dvcfile with artifacts
        if artifacts:
            return next(iter(artifacts.values()))["dvcfile"]
        # return any dvcfile if it exists
        if dvcfiles := self.repo.index._dvcfiles:
            return dvcfiles[0]
        # write to the dvc.yaml file at root
        return os.path.join(self.repo.root_dir, "dvc.yaml")

    def add(self, name, artifact, prefered_dvcfile=None):
        # this doesn't update it "in place", so self.get() won't return the updated value
        # TODO: support writing in `artifacts: artifacts.yaml` case
        # TODO: check `dvcfile` exists - or maybe it's checked in `ProjectFile` already?
        ProjectFile(
            self.repo, self._resolve_dvcfile_path(name, prefered_path=prefered_dvcfile)
        )._dump_pipeline_file_artifacts(name, artifact.to_dict())

    def remove(self, name, prefered_dvcfile=None):
        # TODO: support writing in `artifacts: artifacts.yaml` case
        # TODO: check `dvcfile` exists - or maybe it's checked in `ProjectFile` already?
        ProjectFile(
            self.repo,
            self._resolve_dvcfile_path(
                name, must_exist=True, prefered_path=prefered_dvcfile
            ),
        )._dump_pipeline_file_artifacts(name, None)

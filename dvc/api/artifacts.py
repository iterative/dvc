import os
from typing import Any, Optional

from dvc.repo import Repo


def artifacts_show(
    name: str,
    version: Optional[str] = None,
    stage: Optional[str] = None,
    repo: Optional[str] = None,
) -> dict[str, str]:
    """
    Return path and Git revision for an artifact in a DVC project.

    The resulting path and revision can be used in conjunction with other dvc.api
    calls to open and read the artifact.

    Args:
        name (str): name of the artifact to open.
        version (str, optional): version of the artifact to open. Defaults to
            the latest version.
        stage (str, optional): name of the model registry stage.
        repo: (str, optional): path or URL for the DVC repo.

    Returns:
        Dictionary of the form:
            {
                "rev": ...,
                "path": ...,
            }

    Raises:
        dvc.exceptions.ArtifactNotFoundError: The specified artifact was not found in
            the repo.
    """
    if version and stage:
        raise ValueError("Artifact version and stage are mutually exclusive.")

    from dvc.repo.artifacts import Artifacts
    from dvc.utils import as_posix

    repo_kwargs: dict[str, Any] = {
        "subrepos": True,
        "uninitialized": True,
    }

    dirname, _ = Artifacts.parse_path(name)
    with Repo.open(repo, **repo_kwargs) as _repo:
        rev = _repo.artifacts.get_rev(name, version=version, stage=stage)
        with _repo.switch(rev):
            root = _repo.fs.root_marker
            _dirname = _repo.fs.join(root, dirname) if dirname else root
            with Repo(_dirname, fs=_repo.fs, scm=_repo.scm) as r:
                path = r.artifacts.get_path(name)
                path = _repo.fs.join(_repo.fs.root_marker, as_posix(path))
                parts = _repo.fs.relparts(path, _repo.root_dir)
                return {"rev": rev, "path": os.path.join(*parts)}

import os
import posixpath
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

from dvc.annotations import Artifact
from dvc.dvcfile import PROJECT_FILE
from dvc.exceptions import (
    ArtifactNotFoundError,
    DvcException,
    FileExistsLocallyError,
    InvalidArgumentError,
)
from dvc.log import logger
from dvc.utils import as_posix, relpath, resolve_output
from dvc.utils.objects import cached_property
from dvc.utils.serialize import modify_yaml

if TYPE_CHECKING:
    from gto.tag import Tag as GTOTag
    from scmrepo.git import GitTag

    from dvc.repo import Repo
    from dvc.scm import Git

logger = logger.getChild(__name__)


def check_name_format(name: str) -> None:
    from gto.constants import assert_name_is_valid
    from gto.exceptions import ValidationError

    try:
        assert_name_is_valid(name)
    except ValidationError as exc:
        raise InvalidArgumentError(
            f"Can't use '{name}' as artifact name (ID)."
        ) from exc


def name_is_compatible(name: str) -> bool:
    """
    Only needed by DVCLive per iterative/dvclive#715
    Will be removed in future release.
    """
    from gto.constants import assert_name_is_valid
    from gto.exceptions import ValidationError

    try:
        assert_name_is_valid(name)
        return True
    except ValidationError:
        return False


def check_for_nested_dvc_repo(dvcfile: Path):
    from dvc.repo import Repo

    if dvcfile.is_absolute():
        raise InvalidArgumentError("Use relative path to dvc.yaml.")
    path = dvcfile.parent
    while path.name:
        if (path / Repo.DVC_DIR).is_dir():
            raise InvalidArgumentError(
                f"Nested DVC repos like {path} are not supported."
            )
        path = path.parent


def _reformat_name(name: str) -> str:
    from gto.constants import SEPARATOR_IN_NAME, fullname_re

    # NOTE: DVC accepts names like
    #   path/to/dvc.yaml:artifact_name
    # but Studio/GTO tags are generated with
    #   path/to:artifact_name
    m = fullname_re.match(name)
    if m and m.group("dirname"):
        group = m.group("dirname").rstrip(SEPARATOR_IN_NAME)
        dirname, basename = posixpath.split(group)
        if basename == PROJECT_FILE:
            name = f"{dirname}{SEPARATOR_IN_NAME}{m.group('name')}"
    return name


class Artifacts:
    def __init__(self, repo: "Repo") -> None:
        self.repo = repo

    @cached_property
    def scm(self) -> Optional["Git"]:
        from dvc.scm import Git

        if isinstance(self.repo.scm, Git):
            return self.repo.scm
        return None

    def read(self) -> dict[str, dict[str, Artifact]]:
        """Read artifacts from dvc.yaml."""
        artifacts: dict[str, dict[str, Artifact]] = {}
        for dvcfile, dvcfile_artifacts in self.repo.index._artifacts.items():
            dvcyaml = self.repo.fs.relpath(dvcfile, self.repo.root_dir)
            artifacts[dvcyaml] = {}
            for name, value in dvcfile_artifacts.items():
                try:
                    check_name_format(name)
                except InvalidArgumentError as e:
                    logger.warning(e.msg)
                artifacts[dvcyaml][name] = Artifact(**value)
        return artifacts

    def add(self, name: str, artifact: Artifact, dvcfile: Optional[str] = None):
        """Add artifact to dvc.yaml."""
        with self.repo.scm_context(quiet=True):
            check_name_format(name)
            dvcyaml = Path(dvcfile or PROJECT_FILE)
            check_for_nested_dvc_repo(
                dvcyaml.relative_to(self.repo.root_dir)
                if dvcyaml.is_absolute()
                else dvcyaml
            )

            with modify_yaml(dvcyaml) as data:
                artifacts = data.setdefault("artifacts", {})
                artifacts.update({name: artifact.to_dict()})

            self.repo.scm_context.track_file(dvcfile)

        return artifacts.get(name)

    def get_rev(
        self, name: str, version: Optional[str] = None, stage: Optional[str] = None
    ):
        """Return revision containing the given artifact."""
        from gto.base import sort_versions
        from gto.tag import find, parse_tag

        assert not (version and stage)
        name = _reformat_name(name)
        tags: list[GitTag] = find(name=name, version=version, stage=stage, scm=self.scm)
        if not tags:
            raise ArtifactNotFoundError(name, version=version, stage=stage)
        if version or stage:
            return tags[-1].target
        gto_tags: list[GTOTag] = sort_versions(parse_tag(tag) for tag in tags)
        return gto_tags[0].tag.target

    @classmethod
    def parse_path(cls, name: str) -> tuple[Optional[str], str]:
        from gto.constants import SEPARATOR_IN_NAME, fullname_re

        name = _reformat_name(name)
        m = fullname_re.match(name)
        if not m:
            raise ArtifactNotFoundError(name)
        dirname = m.group("dirname")
        if dirname:
            dirname = dirname.rstrip(SEPARATOR_IN_NAME)

        return dirname, m.group("name")

    def get_path(self, name: str):
        """Return fspath for the given artifact relative to the git root."""
        from dvc.fs import GitFileSystem

        dirname, artifact_name = self.parse_path(name)
        # `name`/`dirname` are expected to be a git root relative.
        # We convert it to dvc-root relative path so that we can read artifacts
        # from dvc.yaml file.
        # But we return dirname intact, as we want to return a git-root relative path.
        # This is useful when reading from `dvcfs` from remote.
        fs = self.repo.fs
        assert self.scm
        if isinstance(fs, GitFileSystem):
            scm_root = fs.root_marker
        else:
            scm_root = self.scm.root_dir

        dirparts = posixpath.normpath(dirname).split(posixpath.sep) if dirname else ()
        abspath = fs.join(scm_root, *dirparts, PROJECT_FILE)
        rela = fs.relpath(abspath, self.repo.root_dir)
        try:
            artifact = self.read()[rela][artifact_name]
        except KeyError as exc:
            raise ArtifactNotFoundError(name) from exc

        path = posixpath.join(dirname or "", artifact.path)
        parts = posixpath.normpath(path).split(posixpath.sep)
        return os.path.join(*parts)

    def download(
        self,
        name: str,
        version: Optional[str] = None,
        stage: Optional[str] = None,
        out: Optional[str] = None,
        force: bool = False,
        jobs: Optional[int] = None,
    ) -> tuple[int, str]:
        """Download the specified artifact."""
        from dvc.fs import download as fs_download
        from dvc.repo import Repo

        logger.debug("Trying to download artifact '%s' via DVC", name)
        rev = self.get_rev(name, version=version, stage=stage)

        dirname, _ = self.parse_path(name)
        with self.repo.switch(rev):
            root = self.repo.fs.root_marker
            _dirname = self.repo.fs.join(root, dirname) if dirname else root
            with Repo(_dirname, fs=self.repo.fs, scm=self.repo.scm) as r:
                path = r.artifacts.get_path(name)
                path = self.repo.fs.join(root, as_posix(path))
                path = self.repo.fs.relpath(path, self.repo.root_dir)
                # when the `repo` is a subrepo, the path `/subrepo/myart.pkl` for dvcfs
                # should be translated as `/myart.pkl`,
                # i.e. relative to the root of the subrepo
                path = self.repo.fs.join(root, path)
                path = self.repo.fs.normpath(path)

            out = resolve_output(path, out, force=force)
            fs = self.repo.dvcfs
            count = fs_download(fs, path, os.path.abspath(out), jobs=jobs)
        return count, out

    @staticmethod
    def _download_studio(
        repo_url: str,
        name: str,
        version: Optional[str] = None,
        stage: Optional[str] = None,
        out: Optional[str] = None,
        force: bool = False,
        jobs: Optional[int] = None,
        dvc_studio_config: Optional[dict[str, Any]] = None,
        **kwargs,
    ) -> tuple[int, str]:
        from dvc.fs import HTTPFileSystem, generic, localfs
        from dvc.fs.callbacks import TqdmCallback
        from dvc_studio_client.model_registry import get_download_uris

        logger.debug("Trying to download artifact '%s' via studio", name)
        out = out or os.getcwd()
        to_infos: list[str] = []
        from_infos: list[str] = []
        if dvc_studio_config is None:
            dvc_studio_config = {}
        dvc_studio_config["repo_url"] = repo_url
        try:
            for path, url in get_download_uris(
                repo_url,
                name,
                version=version,
                stage=stage,
                dvc_studio_config=dvc_studio_config,
                **kwargs,
            ).items():
                to_info = localfs.join(out, path)
                if localfs.exists(to_info) and not force:
                    hint = "\nTo override it, re-run with '--force'."
                    raise FileExistsLocallyError(  # noqa: TRY301
                        relpath(to_info), hint=hint
                    )
                to_infos.append(to_info)
                from_infos.append(url)
        except DvcException:
            raise
        except Exception as exc:
            raise DvcException(
                f"Failed to download artifact '{name}' via Studio"
            ) from exc
        fs = HTTPFileSystem()
        jobs = jobs or fs.jobs
        with TqdmCallback(
            desc=f"Downloading '{name}' from '{repo_url}'",
            unit="files",
        ) as cb:
            cb.set_size(len(from_infos))
            generic.copy(
                fs, from_infos, localfs, to_infos, callback=cb, batch_size=jobs
            )

        return len(to_infos), relpath(localfs.commonpath(to_infos))

    @classmethod
    def get(
        cls,
        url: str,
        name: str,
        version: Optional[str] = None,
        stage: Optional[str] = None,
        config: Optional[Union[str, dict[str, Any]]] = None,
        remote: Optional[str] = None,
        remote_config: Optional[Union[str, dict[str, Any]]] = None,
        out: Optional[str] = None,
        force: bool = False,
        jobs: Optional[int] = None,
    ):
        from dvc.config import Config
        from dvc.repo import Repo

        if version and stage:
            raise InvalidArgumentError(
                "Artifact version and stage are mutually exclusive."
            )

        # NOTE: We try to download the artifact up to three times
        # 1. via studio with studio config loaded from environment
        # 2. via studio with studio config loaded from DVC repo 'studio'
        #    section + environment
        # 3. via DVC remote

        name = _reformat_name(name)
        saved_exc: Optional[Exception] = None

        local_dvc_studio_config = Config().get("studio", {})
        args_dvc_studio_config = {}
        if config and not isinstance(config, dict):
            config = Config.load_file(config)
            args_dvc_studio_config = config.get("studio", {})

        try:
            logger.trace("Trying studio-only config")
            return cls._download_studio(
                url,
                name,
                version=version,
                stage=stage,
                out=out,
                force=force,
                jobs=jobs,
                dvc_studio_config=local_dvc_studio_config | args_dvc_studio_config,
            )
        except FileExistsLocallyError:
            raise
        except Exception as exc:  # noqa: BLE001
            saved_exc = exc

        with Repo.open(
            url=url,
            subrepos=True,
            uninitialized=True,
            config=config,
            remote=remote,
            remote_config=remote_config,
        ) as repo:
            logger.trace("Trying repo [studio] config")
            repo_dvc_studio_config = repo.config.get("studio", {})
            try:
                return cls._download_studio(
                    url,
                    name,
                    version=version,
                    stage=stage,
                    out=out,
                    force=force,
                    jobs=jobs,
                    dvc_studio_config=local_dvc_studio_config
                    | repo_dvc_studio_config
                    | args_dvc_studio_config,
                )
            except FileExistsLocallyError:
                raise
            except Exception as exc:  # noqa: BLE001
                saved_exc = exc

            try:
                return repo.artifacts.download(
                    name,
                    version=version,
                    stage=stage,
                    out=out,
                    force=force,
                    jobs=jobs,
                )
            except FileExistsLocallyError:
                raise
            except Exception as exc:
                if saved_exc:
                    logger.exception(str(saved_exc), exc_info=saved_exc.__cause__)
                raise DvcException(
                    f"Failed to download artifact '{name}' via DVC remote"
                ) from exc

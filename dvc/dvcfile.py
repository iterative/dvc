import contextlib
import logging
import os
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from dvc.exceptions import DvcException
from dvc.stage import serialize
from dvc.stage.exceptions import (
    StageFileBadNameError,
    StageFileDoesNotExistError,
    StageFileIsNotDvcFileError,
)
from dvc.utils import relpath
from dvc.utils.collections import apply_diff
from dvc.utils.objects import cached_property
from dvc.utils.serialize import dump_yaml, modify_yaml

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.types import StrOrBytesPath

    from .parsing import DataResolver
    from .stage import Stage

logger = logging.getLogger(__name__)
_T = TypeVar("_T")

DVC_FILE_SUFFIX = ".dvc"
PROJECT_FILE = "dvc.yaml"
LOCK_FILE = "dvc.lock"


class FileIsGitIgnored(DvcException):
    def __init__(self, path, pipeline_file=False):
        super().__init__(
            "{}'{}' is git-ignored.".format(
                "bad DVC file name " if pipeline_file else "", path
            )
        )


class ParametrizedDumpError(DvcException):
    pass


def is_valid_filename(path):
    return path.endswith(DVC_FILE_SUFFIX) or os.path.basename(path) == PROJECT_FILE


def is_dvc_file(path):
    return os.path.isfile(path) and (is_valid_filename(path) or is_lock_file(path))


def is_lock_file(path):
    return os.path.basename(path) == LOCK_FILE


def is_git_ignored(repo, path):
    from dvc.fs import LocalFileSystem
    from dvc.scm import NoSCMError

    try:
        return isinstance(repo.fs, LocalFileSystem) and repo.scm.is_ignored(path)
    except NoSCMError:
        return False


def check_dvcfile_path(repo, path):
    if not is_valid_filename(path):
        raise StageFileBadNameError(
            "bad DVC file name '{}'. DVC files should be named "
            "'{}' or have a '.dvc' suffix (e.g. '{}.dvc').".format(
                relpath(path), PROJECT_FILE, os.path.basename(path)
            )
        )

    if is_git_ignored(repo, path):
        raise FileIsGitIgnored(relpath(path), True)


class FileMixin:
    SCHEMA: Callable[[_T], _T]

    def __init__(self, repo, path, verify=True, **kwargs):
        self.repo = repo
        self.path = path
        self.verify = verify

    def __repr__(self):
        return f"{self.__class__.__name__}: {relpath(self.path, self.repo.root_dir)}"

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        return self.repo == other.repo and os.path.abspath(
            self.path
        ) == os.path.abspath(other.path)

    def __str__(self):
        return f"{self.__class__.__name__}: {self.relpath}"

    @property
    def relpath(self):
        return relpath(self.path)

    def exists(self):
        is_ignored = self.repo.dvcignore.is_ignored_file(self.path)
        return self.repo.fs.exists(self.path) and not is_ignored

    def _is_git_ignored(self):
        return is_git_ignored(self.repo, self.path)

    def _verify_filename(self):
        if self.verify:
            check_dvcfile_path(self.repo, self.path)

    def _check_gitignored(self):
        if self._is_git_ignored():
            raise FileIsGitIgnored(self.path)

    def load(self, **kwargs: Any) -> Any:
        d, _ = self._load(**kwargs)
        return d

    def _load(self, **kwargs: Any) -> Tuple[Any, str]:
        # it raises the proper exceptions by priority:
        # 1. when the file doesn't exists
        # 2. filename is not a DVC file
        # 3. path doesn't represent a regular file
        # 4. when the file is git ignored
        if not self.exists():
            dvc_ignored = self.repo.dvcignore.is_ignored_file(self.path)
            raise StageFileDoesNotExistError(self.path, dvc_ignored=dvc_ignored)

        self._verify_filename()
        if not self.repo.fs.isfile(self.path):
            raise StageFileIsNotDvcFileError(self.path)

        self._check_gitignored()
        return self._load_yaml(**kwargs)

    @classmethod
    def validate(cls, d: _T, fname: Optional[str] = None) -> _T:
        from dvc.utils.strictyaml import validate

        return validate(d, cls.SCHEMA, path=fname)  # type: ignore[arg-type]

    def _load_yaml(self, **kwargs: Any) -> Tuple[Any, str]:
        from dvc.utils import strictyaml

        return strictyaml.load(
            self.path,
            self.SCHEMA,  # type: ignore[arg-type]
            self.repo.fs,
            **kwargs,
        )

    # pylint: disable-next=unused-argument
    def remove(self, force=False):  # noqa: ARG002
        with contextlib.suppress(FileNotFoundError):
            os.unlink(self.path)

    def dump(self, stage, **kwargs):
        raise NotImplementedError

    def merge(self, ancestor, other, allowed=None):
        raise NotImplementedError


class SingleStageFile(FileMixin):
    from dvc.schema import COMPILED_SINGLE_STAGE_SCHEMA as SCHEMA
    from dvc.stage.loader import SingleStageLoader as LOADER  # noqa: N814

    metrics: List[str] = []
    plots: Any = {}
    params: List[str] = []
    artifacts: Dict[str, Optional[Dict[str, Any]]] = {}

    @property
    def stage(self) -> "Stage":
        data, raw = self._load()
        return self.LOADER.load_stage(self, data, raw)

    @property
    def stages(self) -> LOADER:
        data, raw = self._load()
        return self.LOADER(self, data, raw)

    def dump(self, stage, **kwargs) -> None:
        """Dumps given stage appropriately in the dvcfile."""
        from dvc.stage import PipelineStage

        assert not isinstance(stage, PipelineStage)
        if self.verify:
            check_dvcfile_path(self.repo, self.path)
        logger.debug("Saving information to '%s'.", relpath(self.path))
        dump_yaml(self.path, serialize.to_single_stage_file(stage, **kwargs))
        self.repo.scm_context.track_file(self.relpath)

    # pylint: disable-next=unused-argument
    def remove_stage(self, stage):  # noqa: ARG002
        self.remove()

    def merge(self, ancestor, other, allowed=None):
        assert isinstance(ancestor, SingleStageFile)
        assert isinstance(other, SingleStageFile)

        stage = self.stage
        stage.merge(ancestor.stage, other.stage, allowed=allowed)
        self.dump(stage)


class ProjectFile(FileMixin):
    """Abstraction for pipelines file, .yaml + .lock combined."""

    from dvc.schema import COMPILED_MULTI_STAGE_SCHEMA as SCHEMA
    from dvc.stage.loader import StageLoader as LOADER  # noqa: N814

    @property
    def _lockfile(self):
        return Lockfile(self.repo, os.path.splitext(self.path)[0] + ".lock")

    def _reset(self):
        self.__dict__.pop("contents", None)
        self.__dict__.pop("lockfile_contents", None)
        self.__dict__.pop("resolver", None)
        self.__dict__.pop("stages", None)

    def dump(
        self, stage, update_pipeline=True, update_lock=True, **kwargs
    ):  # pylint: disable=arguments-differ
        """Dumps given stage appropriately in the dvcfile."""
        from dvc.stage import PipelineStage

        assert isinstance(stage, PipelineStage)
        if self.verify:
            check_dvcfile_path(self.repo, self.path)

        if update_pipeline and not stage.is_data_source:
            self._dump_pipeline_file(stage)

        if update_lock:
            self._dump_lockfile(stage, **kwargs)

    def _dump_lockfile(self, stage, **kwargs):
        self._lockfile.dump(stage, **kwargs)

    @staticmethod
    def _check_if_parametrized(stage, action: str = "dump") -> None:
        if stage.raw_data.parametrized:
            raise ParametrizedDumpError(f"cannot {action} a parametrized {stage}")

    def _dump_pipeline_file(self, stage):
        self._check_if_parametrized(stage)
        stage_data = serialize.to_pipeline_file(stage)

        with modify_yaml(self.path, fs=self.repo.fs) as data:
            if not data:
                logger.info("Creating '%s'", self.relpath)

            data["stages"] = data.get("stages", {})
            existing_entry = stage.name in data["stages"]
            action = "Modifying" if existing_entry else "Adding"
            logger.info("%s stage '%s' in '%s'", action, stage.name, self.relpath)

            if existing_entry:
                orig_stage_data = data["stages"][stage.name]
                apply_diff(stage_data[stage.name], orig_stage_data)
            else:
                data["stages"].update(stage_data)

        self.repo.scm_context.track_file(self.relpath)

    @property
    def stage(self):
        raise DvcException("ProjectFile has multiple stages. Please specify it's name.")

    @cached_property
    def contents(self) -> Dict[str, Any]:
        return self._load()[0]

    @cached_property
    def lockfile_contents(self) -> Dict[str, Any]:
        return self._lockfile.load()

    @cached_property
    def resolver(self) -> "DataResolver":
        from .parsing import DataResolver

        wdir = self.repo.fs.path.parent(self.path)
        return DataResolver(self.repo, wdir, self.contents)

    @cached_property
    def stages(self) -> LOADER:
        return self.LOADER(self, self.contents, self.lockfile_contents)

    @property
    def metrics(self) -> List[str]:
        return self.contents.get("metrics", [])

    @property
    def plots(self) -> Any:
        return self.contents.get("plots", {})

    @property
    def params(self) -> List[str]:
        return self.contents.get("params", [])

    @property
    def artifacts(self) -> Dict[str, Optional[Dict[str, Any]]]:
        return self.contents.get("artifacts", {})

    def remove(self, force=False):
        if not force:
            logger.warning("Cannot remove pipeline file.")
            return

        super().remove()
        self._lockfile.remove()

    def remove_stage(self, stage):
        self._check_if_parametrized(stage, "remove")
        self._lockfile.remove_stage(stage)
        if not self.exists():
            return

        d, _ = self._load_yaml(round_trip=True)
        if stage.name not in d.get("stages", {}):
            return

        logger.debug("Removing '%s' from '%s'", stage.name, self.path)
        del d["stages"][stage.name]

        if d["stages"]:
            dump_yaml(self.path, d)
        else:
            super().remove()

    def merge(self, ancestor, other, allowed=None):
        raise NotImplementedError


class Lockfile(FileMixin):
    from dvc.schema import COMPILED_LOCKFILE_SCHEMA as SCHEMA

    def _verify_filename(self):
        pass  # lockfile path is hardcoded, so no need to verify here

    def _load(self, **kwargs: Any):
        try:
            return super()._load(**kwargs)
        except StageFileDoesNotExistError:
            # we still need to account for git-ignored dvc.lock file
            # even though it may not exist or have been .dvcignored
            self._check_gitignored()
            return {}, ""

    def dump(self, stage, **kwargs):
        stage_data = serialize.to_lockfile(stage, **kwargs)

        with modify_yaml(self.path, fs=self.repo.fs) as data:
            if not data:
                data.update({"schema": "2.0"})
                # order is important, meta should always be at the top
                logger.info("Generating lock file '%s'", self.relpath)

            data["stages"] = data.get("stages", {})
            modified = data["stages"].get(stage.name, {}) != stage_data.get(
                stage.name, {}
            )
            if modified:
                logger.info("Updating lock file '%s'", self.relpath)

            data["stages"].update(stage_data)

        if modified:
            self.repo.scm_context.track_file(self.relpath)

    def remove_stage(self, stage):
        if not self.exists():
            return

        d, _ = self._load_yaml(round_trip=True)
        data = d.get("stages", {})
        if stage.name not in data:
            return

        logger.debug("Removing '%s' from '%s'", stage.name, self.path)
        del data[stage.name]

        if data:
            dump_yaml(self.path, d)
        else:
            self.remove()

    def merge(self, ancestor, other, allowed=None):
        raise NotImplementedError


def load_file(
    repo: "Repo", path: "StrOrBytesPath", **kwargs: Any
) -> Union[ProjectFile, SingleStageFile]:
    _, ext = os.path.splitext(path)
    if ext in (".yaml", ".yml"):
        return ProjectFile(repo, path, **kwargs)
    return SingleStageFile(repo, path, **kwargs)

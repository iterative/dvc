import logging
import os
from collections import defaultdict
from contextlib import contextmanager
from functools import wraps
from typing import TYPE_CHECKING, Callable, Optional

from funcy import cached_property

from dvc.exceptions import FileMissingError
from dvc.exceptions import IsADirectoryError as DvcIsADirectoryError
from dvc.exceptions import NotDvcRepoError, OutputNotFoundError
from dvc.ignore import DvcIgnoreFilter
from dvc.utils import env2bool
from dvc.utils.fs import path_isin

if TYPE_CHECKING:
    from dvc.fs import FileSystem
    from dvc.repo.scm_context import SCMContext
    from dvc.scm import Base

logger = logging.getLogger(__name__)


@contextmanager
def lock_repo(repo: "Repo"):
    # pylint: disable=protected-access
    depth = repo._lock_depth
    repo._lock_depth += 1

    try:
        if depth > 0:
            yield
        else:
            with repo.lock:
                repo._reset()
                yield
                # Graph cache is no longer valid after we release the repo.lock
                repo._reset()
    finally:
        repo._lock_depth = depth


def locked(f):
    @wraps(f)
    def wrapper(repo, *args, **kwargs):
        with lock_repo(repo):
            return f(repo, *args, **kwargs)

    return wrapper


class Repo:
    DVC_DIR = ".dvc"

    from dvc.repo.add import add  # type: ignore[misc]
    from dvc.repo.checkout import checkout  # type: ignore[misc]
    from dvc.repo.commit import commit  # type: ignore[misc]
    from dvc.repo.destroy import destroy  # type: ignore[misc]
    from dvc.repo.diff import diff  # type: ignore[misc]
    from dvc.repo.fetch import fetch  # type: ignore[misc]
    from dvc.repo.freeze import freeze, unfreeze  # type: ignore[misc]
    from dvc.repo.gc import gc  # type: ignore[misc]
    from dvc.repo.get import get as _get  # type: ignore[misc]
    from dvc.repo.get_url import get_url as _get_url  # type: ignore[misc]
    from dvc.repo.imp import imp  # type: ignore[misc]
    from dvc.repo.imp_url import imp_url  # type: ignore[misc]
    from dvc.repo.install import install  # type: ignore[misc]
    from dvc.repo.ls import ls as _ls  # type: ignore[misc]
    from dvc.repo.ls_url import ls_url as _ls_url  # type: ignore[misc]
    from dvc.repo.move import move  # type: ignore[misc]
    from dvc.repo.pull import pull  # type: ignore[misc]
    from dvc.repo.push import push  # type: ignore[misc]
    from dvc.repo.remove import remove  # type: ignore[misc]
    from dvc.repo.reproduce import reproduce  # type: ignore[misc]
    from dvc.repo.run import run  # type: ignore[misc]
    from dvc.repo.status import status  # type: ignore[misc]
    from dvc.repo.update import update  # type: ignore[misc]

    from .data import status as data_status  # type: ignore[misc]

    ls = staticmethod(_ls)
    ls_url = staticmethod(_ls_url)
    get = staticmethod(_get)
    get_url = staticmethod(_get_url)

    def _get_repo_dirs(
        self,
        root_dir: str = None,
        fs: "FileSystem" = None,
        uninitialized: bool = False,
        scm: "Base" = None,
    ):
        from dvc.fs import localfs
        from dvc.scm import SCM, SCMError

        dvc_dir = None
        tmp_dir = None
        try:
            root_dir = self.find_root(root_dir, fs)
            fs = fs or localfs
            dvc_dir = fs.path.join(root_dir, self.DVC_DIR)
            tmp_dir = fs.path.join(dvc_dir, "tmp")
        except NotDvcRepoError:
            if not uninitialized:
                raise

            if not scm:
                try:
                    scm = SCM(root_dir or os.curdir)
                except SCMError:
                    scm = SCM(os.curdir, no_scm=True)

            if not fs or not root_dir:
                root_dir = scm.root_dir

        assert root_dir
        return root_dir, dvc_dir, tmp_dir

    def _get_database_dir(self, db_name):
        # NOTE: by default, store SQLite-based remote indexes and state's
        # `links` and `md5s` caches in the repository itself to avoid any
        # possible state corruption in 'shared cache dir' scenario, but allow
        # user to override this through config when, say, the repository is
        # located on a mounted volume — see
        # https://github.com/iterative/dvc/issues/4420
        base_db_dir = self.config.get(db_name, {}).get("dir", None)
        if not base_db_dir:
            return self.tmp_dir

        import hashlib

        root_dir_hash = hashlib.sha224(
            self.root_dir.encode("utf-8")
        ).hexdigest()

        db_dir = self.fs.path.join(
            base_db_dir,
            self.DVC_DIR,
            f"{self.fs.path.name(self.root_dir)}-{root_dir_hash[0:7]}",
        )

        self.fs.makedirs(db_dir, exist_ok=True)
        return db_dir

    def __init__(
        self,
        root_dir=None,
        fs=None,
        rev=None,
        subrepos=False,
        uninitialized=False,
        config=None,
        url=None,
        repo_factory=None,
        scm=None,
    ):
        from dvc.config import Config
        from dvc.data_cloud import DataCloud
        from dvc.fs import GitFileSystem, LocalFileSystem, localfs
        from dvc.lock import LockNoop, make_lock
        from dvc.odbmgr import ODBManager
        from dvc.repo.metrics import Metrics
        from dvc.repo.params import Params
        from dvc.repo.plots import Plots
        from dvc.repo.stage import StageLoad
        from dvc.scm import SCM
        from dvc.stage.cache import StageCache
        from dvc_data.hashfile.state import State, StateNoop

        self.url = url
        self._fs_conf = {"repo_factory": repo_factory}
        self._fs = fs or localfs
        self._scm = scm

        if rev and not fs:
            self._scm = scm = SCM(root_dir or os.curdir)
            root_dir = "/"
            self._fs = GitFileSystem(scm=self._scm, rev=rev)

        self.root_dir, self.dvc_dir, self.tmp_dir = self._get_repo_dirs(
            root_dir=root_dir,
            fs=self.fs,
            uninitialized=uninitialized,
            scm=scm,
        )

        self.config = Config(self.dvc_dir, fs=self.fs, config=config)
        self._uninitialized = uninitialized

        # used by DVCFileSystem to determine if it should traverse subrepos
        self.subrepos = subrepos

        self.cloud = DataCloud(self)
        self.stage = StageLoad(self)

        if isinstance(self.fs, GitFileSystem) or not self.dvc_dir:
            self.lock = LockNoop()
            self.state = StateNoop()
            self.odb = ODBManager(self)
            self.tmp_dir = None
        else:
            self.fs.makedirs(self.tmp_dir, exist_ok=True)

            if isinstance(self.fs, LocalFileSystem):
                self.lock = make_lock(
                    self.fs.path.join(self.tmp_dir, "lock"),
                    tmp_dir=self.tmp_dir,
                    hardlink_lock=self.config["core"].get(
                        "hardlink_lock", False
                    ),
                    friendly=True,
                )
                state_db_dir = self._get_database_dir("state")
                self.state = State(self.root_dir, state_db_dir, self.dvcignore)
            else:
                self.lock = LockNoop()
                self.state = StateNoop()

            self.odb = ODBManager(self)

            self.stage_cache = StageCache(self)

            self._ignore()

        self.metrics = Metrics(self)
        self.plots = Plots(self)
        self.params = Params(self)

        self.stage_collection_error_handler: Optional[
            Callable[[str, Exception], None]
        ] = None
        self._lock_depth = 0

    def __str__(self):
        return self.url or self.root_dir

    @cached_property
    def index(self):
        from dvc.repo.index import Index

        return Index(self)

    @staticmethod
    def open(url, *args, **kwargs):
        if url is None:
            url = os.getcwd()

        if os.path.exists(url):
            try:
                return Repo(url, *args, **kwargs)
            except NotDvcRepoError:
                pass  # fallthrough to external_repo

        from dvc.external_repo import external_repo

        return external_repo(url, *args, **kwargs)

    @cached_property
    def scm(self):
        from dvc.scm import SCM, SCMError

        if self._scm:
            return self._scm

        no_scm = self.config["core"].get("no_scm", False)
        try:
            return SCM(self.root_dir, no_scm=no_scm)
        except SCMError:
            if self._uninitialized:
                # might not be a git/dvc repo at all
                # used in `params/metrics/plots/live` targets
                return SCM(self.root_dir, no_scm=True)
            raise

    @cached_property
    def scm_context(self) -> "SCMContext":
        from dvc.repo.scm_context import SCMContext

        return SCMContext(self.scm, self.config)

    @cached_property
    def dvcignore(self) -> DvcIgnoreFilter:

        return DvcIgnoreFilter(self.fs, self.root_dir)

    def get_rev(self):
        from dvc.fs import LocalFileSystem

        assert self.scm
        if isinstance(self.fs, LocalFileSystem):
            from dvc.scm import map_scm_exception

            with map_scm_exception():
                return self.scm.get_rev()
        return self.fs.rev

    @cached_property
    def experiments(self):
        from dvc.repo.experiments import Experiments

        return Experiments(self)

    @cached_property
    def machine(self):
        from dvc.machine import MachineManager

        if self.tmp_dir and (
            self.config["feature"].get("machine", False)
            or env2bool("DVC_TEST")
        ):
            return MachineManager(self)
        return None

    @property
    def fs(self) -> "FileSystem":
        return self._fs

    @fs.setter
    def fs(self, fs: "FileSystem"):
        self._fs = fs
        # Our graph cache is no longer valid, as it was based on the previous
        # fs.
        self._reset()

    def __repr__(self):
        return f"{self.__class__.__name__}: '{self.root_dir}'"

    @classmethod
    def find_root(cls, root=None, fs=None) -> str:
        from dvc.fs import LocalFileSystem, localfs

        fs = fs or localfs
        root = root or os.curdir
        root_dir = fs.path.realpath(root)

        if not fs.isdir(root_dir):
            raise NotDvcRepoError(f"directory '{root}' does not exist")

        while True:
            dvc_dir = fs.path.join(root_dir, cls.DVC_DIR)
            if fs.isdir(dvc_dir):
                return root_dir
            if isinstance(fs, LocalFileSystem) and os.path.ismount(root_dir):
                break
            parent = fs.path.parent(root_dir)
            if parent == root_dir:
                break
            root_dir = parent

        msg = "you are not inside of a DVC repository"

        if isinstance(fs, LocalFileSystem):
            msg = f"{msg} (checked up to mount point '{root_dir}')"

        raise NotDvcRepoError(msg)

    @classmethod
    def find_dvc_dir(cls, root=None):
        root_dir = cls.find_root(root)
        return os.path.join(root_dir, cls.DVC_DIR)

    @staticmethod
    def init(root_dir=os.curdir, no_scm=False, force=False, subdir=False):
        from dvc.repo.init import init

        return init(
            root_dir=root_dir, no_scm=no_scm, force=force, subdir=subdir
        )

    def unprotect(self, target):
        return self.odb.repo.unprotect(target)

    def _ignore(self):
        flist = [self.config.files["local"], self.tmp_dir]

        if path_isin(self.odb.repo.path, self.root_dir):
            flist += [self.odb.repo.path]

        for file in flist:
            self.scm_context.ignore(file)

    def brancher(self, *args, **kwargs):
        from dvc.repo.brancher import brancher

        return brancher(self, *args, **kwargs)

    def used_objs(
        self,
        targets=None,
        all_branches=False,
        with_deps=False,
        all_tags=False,
        all_commits=False,
        all_experiments=False,
        commit_date: Optional[str] = None,
        remote=None,
        force=False,
        jobs=None,
        recursive=False,
        used_run_cache=None,
        revs=None,
        num=1,
    ):
        """Get the stages related to the given target and collect
        the `info` of its outputs.

        This is useful to know what files from the cache are _in use_
        (namely, a file described as an output on a stage).

        The scope is, by default, the working directory, but you can use
        `all_branches`/`all_tags`/`all_commits`/`all_experiments` to expand
        the scope.

        Returns:
            A dict mapping (remote) ODB instances to sets of objects that
            belong to each ODB. If the ODB instance is None, the objects
            are naive and do not belong to a specific remote ODB.
        """
        used = defaultdict(set)

        for _ in self.brancher(
            revs=revs,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            all_experiments=all_experiments,
            commit_date=commit_date,
            num=num,
        ):
            for odb, objs in self.index.used_objs(
                targets,
                remote=remote,
                force=force,
                jobs=jobs,
                recursive=recursive,
                with_deps=with_deps,
            ).items():
                used[odb].update(objs)

        if used_run_cache:
            for odb, objs in self.stage_cache.get_used_objs(
                used_run_cache, remote=remote, force=force, jobs=jobs
            ).items():
                used[odb].update(objs)

        return used

    @property
    def stages(self):  # obsolete, only for backward-compatibility
        return self.index.stages

    def find_outs_by_path(self, path, outs=None, recursive=False, strict=True):
        # using `outs_graph` to ensure graph checks are run
        outs = outs or self.index.outs_graph

        abs_path = self.fs.path.abspath(path)
        fs_path = abs_path

        def func(out):
            def eq(one, two):
                return one == two

            match = eq if strict else out.fs.path.isin_or_eq

            if out.protocol == "local" and match(fs_path, out.fs_path):
                return True

            if recursive and out.fs.path.isin(out.fs_path, fs_path):
                return True

            return False

        matched = list(filter(func, outs))
        if not matched:
            raise OutputNotFoundError(path, self)

        return matched

    def is_dvc_internal(self, path):
        path_parts = self.fs.path.normpath(path).split(self.fs.sep)
        return self.DVC_DIR in path_parts

    @cached_property
    def datafs(self):
        from dvc.fs.data import DataFileSystem

        return DataFileSystem(index=self.index.data["repo"])

    @cached_property
    def dvcfs(self):
        from dvc.fs.dvc import DVCFileSystem

        return DVCFileSystem(
            repo=self, subrepos=self.subrepos, **self._fs_conf
        )

    @cached_property
    def index_db_dir(self):
        return self._get_database_dir("index")

    @contextmanager
    def open_by_relpath(self, path, remote=None, mode="r", encoding=None):
        """Opens a specified resource as a file descriptor"""
        from dvc.fs.data import DataFileSystem
        from dvc.fs.dvc import DVCFileSystem

        if os.path.isabs(path):
            fs = DataFileSystem(index=self.index.data["local"])
            fs_path = path
        else:
            fs = DVCFileSystem(repo=self, subrepos=True)
            fs_path = fs.from_os_path(path)

        try:
            if remote:
                remote_odb = self.cloud.get_remote_odb(name=remote)
                oid = fs.info(fs_path)["dvc_info"]["md5"]
                fs = remote_odb.fs
                fs_path = remote_odb.oid_to_path(oid)

            with fs.open(
                fs_path,
                mode=mode,
                encoding=encoding,
            ) as fobj:
                yield fobj
        except FileNotFoundError as exc:
            raise FileMissingError(path) from exc
        except IsADirectoryError as exc:
            raise DvcIsADirectoryError(f"'{path}' is a directory") from exc

    def close(self):
        self.scm.close()
        self.state.close()

    def _reset(self):
        self.state.close()
        self.scm._reset()  # pylint: disable=protected-access
        self.__dict__.pop("index", None)
        self.__dict__.pop("dvcignore", None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._reset()
        self.scm.close()

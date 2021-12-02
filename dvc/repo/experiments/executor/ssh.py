import logging
import posixpath
from contextlib import contextmanager
from typing import TYPE_CHECKING, Callable, Optional

from funcy import first

from dvc.fs.ssh import SSHFileSystem
from dvc.repo.experiments.base import (
    EXEC_BRANCH,
    EXEC_CHECKPOINT,
    EXEC_HEAD,
    EXEC_MERGE,
    EXEC_NAMESPACE,
)

from .base import BaseExecutor

if TYPE_CHECKING:
    from scmrepo.git import Git

    from dvc.machine import MachineManager
    from dvc.repo import Repo

    from ..base import ExpStashEntry

logger = logging.getLogger(__name__)


@contextmanager
def _sshfs(fs_factory, **kwargs):
    if fs_factory:
        with fs_factory() as fs:
            yield fs
        return
    yield SSHFileSystem(**kwargs)


class SSHExecutor(BaseExecutor):
    """SSH experiment executor."""

    WARN_UNTRACKED = True
    QUIET = True

    def __init__(
        self,
        *args,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        fs_factory: Optional[Callable] = None,
        **kwargs,
    ):
        assert host

        super().__init__(*args, **kwargs)
        self.host: str = host
        self.port = port
        self.username = username
        self._fs_factory = fs_factory
        self._repo_abspath = ""

    @classmethod
    def gen_dirname(cls, name: Optional[str] = None):
        from shortuuid import uuid

        return "-".join([name or "dvc-exp", "executor", uuid()])

    @classmethod
    def from_stash_entry(
        cls,
        repo: "Repo",
        stash_rev: str,
        entry: "ExpStashEntry",
        **kwargs,
    ):
        manager: "MachineManager" = kwargs.pop("manager")
        machine_name: Optional[str] = kwargs.pop("machine_name", None)
        executor = cls._from_stash_entry(
            repo,
            stash_rev,
            entry,
            cls.gen_dirname(entry.name),
            **manager.get_executor_kwargs(machine_name),
        )
        logger.debug("Init SSH executor for host '%s'", executor.host)
        return executor

    def sshfs(self):
        return _sshfs(self._fs_factory, host=self.host, port=self.port)

    @property
    def git_url(self) -> str:
        user = f"{self.username}@" if self.username else ""
        port = f":{self.port}" if self.port is not None else ""
        path = f"{self.root_dir}" if self.root_dir else ""
        if path and not posixpath.isabs(path):
            path = f"/~/{path}"
        return f"ssh://{user}{self.host}{port}{path}"

    @property
    def abs_url(self) -> str:
        assert self._repo_abspath
        user = f"{self.username}@" if self.username else ""
        port = f":{self.port}" if self.port is not None else ""
        return f"ssh://{user}{self.host}{port}{self._repo_abspath}"

    @staticmethod
    def _git_client_args(fs):
        kwargs = {
            "password": fs.fs_args.get("password"),
            "key_filename": first(fs.fs_args.get("client_keys", [])),
        }
        return kwargs

    def init_git(self, scm: "Git", branch: Optional[str] = None):
        from ..utils import push_refspec

        with self.sshfs() as fs:
            fs.makedirs(self.root_dir)
            self._ssh_cmd(fs, "git init .")
            self._ssh_cmd(fs, "git config user.name dvc-exp")
            self._ssh_cmd(
                fs, "git config user.email dvc-exp@noreply.localhost"
            )

            result = self._ssh_cmd(fs, "pwd")
            path = result.stdout.strip()
            self._repo_abspath = path

            # TODO: support multiple client key retries in git backends
            # (see https://github.com/iterative/dvc/issues/6508)
            kwargs = self._git_client_args(fs)
            refspec = f"{EXEC_NAMESPACE}/"
            push_refspec(scm, self.git_url, refspec, refspec, **kwargs)
            if branch:
                push_refspec(scm, self.git_url, branch, branch, **kwargs)
                self._ssh_cmd(fs, f"git symbolic-ref {EXEC_BRANCH} {branch}")
            else:
                self._ssh_cmd(
                    fs, f"git symbolic-ref -d {EXEC_BRANCH}", check=False
                )
            self._ssh_cmd(
                fs, f"git update-ref -d {EXEC_CHECKPOINT}", check=False
            )

            # checkout EXEC_HEAD and apply EXEC_MERGE on top of it without
            # committing
            head = EXEC_BRANCH if branch else EXEC_HEAD
            self._ssh_cmd(fs, f"git checkout {head}")
            merge_rev = scm.get_ref(EXEC_MERGE)
            self._ssh_cmd(fs, f"git merge --squash --no-commit {merge_rev}")

    def _ssh_cmd(self, sshfs, cmd, chdir=None, **kwargs):
        working_dir = chdir or self.root_dir
        return sshfs.fs.execute(f"cd {working_dir};{cmd}", **kwargs)

    def init_cache(self, dvc: "Repo", rev: str, run_cache: bool = True):
        from dvc.objects.db import ODBManager, get_odb
        from dvc.repo import Repo
        from dvc.repo.push import push

        cache_path = posixpath.join(
            self._repo_abspath,
            Repo.DVC_DIR,
            ODBManager.CACHE_DIR,
        )

        with self.sshfs() as fs:
            odb = get_odb(fs, cache_path, **fs.config)
            push(dvc, revs=[rev], run_cache=run_cache, odb=odb)

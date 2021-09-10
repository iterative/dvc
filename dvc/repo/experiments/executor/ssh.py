import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Callable, Optional

# from funcy import cached_property, first
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
    # from multiprocessing import Queue

    from dvc.machine import MachineManager
    from dvc.scm.git import Git

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

        self.host: str = host
        self.port = port
        self.username = username
        self._fs_factory = fs_factory

        super().__init__(*args, **kwargs)
        logger.debug("Init SSH executor for host '%s'", self.host)

    @classmethod
    def from_machine(
        cls,
        manager: "MachineManager",
        machine_name: Optional[str],
        *args,
        **kwargs,
    ):
        from shortuuid import uuid

        name = kwargs.get("name") or "dvc-exp"
        kwargs["root_dir"] = f"{name}-executor-{uuid()}"
        kwargs.update(manager.get_executor_kwargs(machine_name))
        return cls(*args, **kwargs)

    def sshfs(self):
        return _sshfs(self._fs_factory, host=self.host, port=self.port)

    @property
    def git_url(self) -> str:
        user = f"{self.username}@" if self.username else ""
        port = f":{self.port}" if self.port is not None else ""
        path = f"{self.root_dir}" if self.root_dir else ""
        return f"{user}{self.host}{port}:{path}"

    @staticmethod
    def _git_client_args(fs):
        return {
            "password": fs.fs_args.get("password"),
            "key_filename": first(fs.fs_args.get("client_keys", [])),
        }

    def _init_git(self, scm: "Git", branch: Optional[str] = None, **kwargs):
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
            kwargs.update(self._git_client_args(fs))
            refspec = f"{EXEC_NAMESPACE}/"
            scm.push_refspec(self.git_url, refspec, refspec, **kwargs)
            if branch:
                scm.push_refspec(self.git_url, branch, branch, **kwargs)
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

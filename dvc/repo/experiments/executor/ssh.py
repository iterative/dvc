import logging
import posixpath
import sys
from contextlib import contextmanager
from typing import TYPE_CHECKING, Callable, Iterable, Optional

from funcy import first

from dvc.fs.ssh import SSHFileSystem
from dvc.repo.experiments.base import (
    EXEC_BRANCH,
    EXEC_CHECKPOINT,
    EXEC_HEAD,
    EXEC_MERGE,
    EXEC_NAMESPACE,
)

from .base import BaseExecutor, ExecutorResult

if TYPE_CHECKING:
    from multiprocessing import Queue

    from dvc.machine import MachineManager
    from dvc.repo import Repo
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
    def gen_dirname(cls, name: Optional[str] = None):
        from shortuuid import uuid

        return "-".join([name or "dvc-exp", "executor", uuid()])

    @classmethod
    def from_machine(
        cls,
        manager: "MachineManager",
        machine_name: Optional[str],
        *args,
        **kwargs,
    ):
        kwargs["root_dir"] = cls.gen_dirname(kwargs.get("name"))
        kwargs.update(manager.get_executor_kwargs(machine_name))
        return cls(*args, **kwargs)

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

    def cleanup(self):
        pass

    @contextmanager
    def remote_odb(self):
        from dvc.objects.db import get_odb

        with self.sshfs() as fs:
            url = self.abs_url / ".dvc" / "cache"
            fs.makedirs(url)
            yield get_odb(fs, url)

    @staticmethod
    def _git_client_args(fs):
        kwargs = {
            "password": fs.fs_args.get("password"),
            "key_filename": first(fs.fs_args.get("client_keys", [])),
        }
        return kwargs

    def _init_git(self, scm: "Git", branch: Optional[str] = None, **kwargs):
        logger.debug("Init remote git repo '%s'", self.git_url)
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

    def init_cache(self, dvc: "Repo", rev: str, run_cache: bool = True):
        from dvc.objects.db import ODBManager, get_odb
        from dvc.repo import Repo
        from dvc.repo.push import push

        cache_path = posixpath.join(
            self._repo_abspath,
            Repo.DVC_DIR,
            ODBManager.CACHE_DIR,
        )
        logger.debug("Init remote DVC cache '%s'", cache_path)
        with self.sshfs() as fs:
            odb = get_odb(fs, cache_path, **fs.config)
            push(
                dvc,
                revs=[rev],
                run_cache=run_cache,
                odb=odb,
                include_imports=True,
            )

    def collect_exps(self, dest_scm: "Git", **kwargs) -> Iterable[str]:
        # TODO: pull DVC cache
        logger.debug("Collect remote Git exps from '%s'", self.git_url)
        with self.sshfs() as fs:
            kwargs.update(self._git_client_args(fs))
            return super().collect_exps(dest_scm, **kwargs)

    @property
    def worker_kwargs(self):
        return {
            "fs_factory": self._fs_factory,
            "repo_path": self._repo_abspath,
        }

    @classmethod
    def reproduce(
        cls,
        dvc_dir: Optional[str],
        rev: str,
        queue: Optional["Queue"] = None,
        rel_cwd: Optional[str] = None,
        name: Optional[str] = None,
        log_errors: bool = True,
        log_level: Optional[int] = None,
        **kwargs,
    ) -> "ExecutorResult":
        from asyncssh import ProcessError

        fs_factory: Optional[Callable] = kwargs.pop("fs_factory", None)
        repo_path: str = kwargs.pop("repo_path")

        if log_errors and log_level is not None:
            cls._set_log_level(log_level)

        with _sshfs(fs_factory) as fs:
            logger.info(
                "Reproducing experiment on '%s'", fs.fs_args.get("host")
            )
            args_url = posixpath.join(
                repo_path, ".dvc", "tmp", cls.PACKED_ARGS_FILE
            )
            if fs.exists(args_url):
                _args, kwargs = cls.unpack_repro_args(args_url, fs=fs)
                fs.fs.execute(
                    (
                        f"cd {repo_path};"
                        "git rm .dvc/tmp/{cls.PACKED_ARGS_FILE}"
                    ),
                    check=False,
                )
            else:
                # args = []
                kwargs = {}

            logger.debug("Configuring remote venv...")
            result = fs.fs.execute(
                f"cd {repo_path};python3 -m venv .env",
                stdout=sys.stdout,
                stderr=sys.stderr,
            )
            result = fs.fs.execute(
                f"cd {repo_path};"
                ".env/bin/python -m pip install -r src/requirements.txt",
                stdout=sys.stdout,
                stderr=sys.stderr,
            )

            # TODO: handle args/kwargs
            logger.debug("Calling 'dvc exp run'...")
            # TODO: nohup (?) output and allow detach/attach
            try:
                result = fs.fs.execute(
                    f"cd {repo_path};source .env/bin/activate;dvc exp run",
                    stdout=sys.stdout,
                    stderr=sys.stderr,
                )
            except ProcessError:
                return ExecutorResult(None, None, False)

            result = fs.fs.execute(
                f"cd {repo_path};git symbolic-ref {EXEC_BRANCH}"
            )
            exp_ref = result.stdout.strip()
            # TODO: return valid exp hash
            return ExecutorResult("TODO", exp_ref, kwargs.get("force", False))

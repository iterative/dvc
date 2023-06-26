import logging
import os
import posixpath
import sys
from contextlib import contextmanager
from typing import TYPE_CHECKING, Callable, Iterable, List, Optional

from dvc_ssh import SSHFileSystem
from funcy import first

from dvc.repo.experiments.refs import (
    EXEC_BASELINE,
    EXEC_BRANCH,
    EXEC_HEAD,
    EXEC_MERGE,
    EXEC_NAMESPACE,
)

from .base import BaseExecutor, ExecutorInfo, ExecutorResult, TaskStatus

if TYPE_CHECKING:
    from queue import Queue

    from dvc.repo import Repo
    from dvc.repo.experiments.refs import ExpRefInfo
    from dvc.repo.experiments.stash import ExpStashEntry
    from dvc.scm import Git

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
    SETUP_SCRIPT_FILENAME = "exec-setup.sh"

    def __init__(
        self,
        *args,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        fs_factory: Optional[Callable] = None,
        setup_script: Optional[str] = None,
        **kwargs,
    ):
        assert host

        super().__init__(*args, **kwargs)
        self.host: str = host
        self.port = port
        self.username = username
        self._fs_factory = fs_factory
        self._repo_abspath = ""
        self._setup_script = setup_script

    @classmethod
    def gen_dirname(cls, name: Optional[str] = None):
        from shortuuid import uuid

        return "-".join([name or "dvc-exp", "executor", uuid()])

    @classmethod
    def from_stash_entry(
        cls,
        repo: "Repo",
        entry: "ExpStashEntry",
        **kwargs,
    ):
        machine_name: Optional[str] = kwargs.pop("machine_name", None)
        assert repo.machine
        executor = cls._from_stash_entry(
            repo,
            entry,
            cls.gen_dirname(entry.name),
            location=machine_name,
            **repo.machine.get_executor_kwargs(machine_name),
            setup_script=repo.machine.get_setup_script(machine_name),
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
        return {
            "password": fs.fs_args.get("password"),
            "key_filename": first(fs.fs_args.get("client_keys", [])),
        }

    def init_git(
        self,
        repo: "Repo",  # noqa: ARG002
        scm: "Git",
        stash_rev: str,
        entry: "ExpStashEntry",
        infofile: Optional[str],
        branch: Optional[str] = None,
    ):
        from dvc.repo.experiments.utils import push_refspec

        self.status = TaskStatus.PREPARING
        if infofile:
            self.info.dump_json(infofile)

        with self.sshfs() as fs:
            fs.makedirs(self.root_dir)
            self._ssh_cmd(fs, "git init .")
            self._ssh_cmd(fs, "git config user.name dvc-exp")
            self._ssh_cmd(fs, "git config user.email dvc-exp@noreply.localhost")

            result = self._ssh_cmd(fs, "pwd")
            path = result.stdout.strip()
            self._repo_abspath = path

            # TODO: support multiple client key retries in git backends
            # (see https://github.com/iterative/dvc/issues/6508)
            kwargs = self._git_client_args(fs)

            ref_dict = {
                EXEC_HEAD: entry.head_rev,
                EXEC_MERGE: stash_rev,
                EXEC_BASELINE: entry.baseline_rev,
            }
            with self.set_temp_refs(scm, ref_dict):
                exec_namespace = f"{EXEC_NAMESPACE}/"
                refspec = [(exec_namespace, exec_namespace)]
                push_refspec(scm, self.git_url, refspec, **kwargs)

            if branch:
                push_refspec(scm, self.git_url, [(branch, branch)], **kwargs)
                self._ssh_cmd(fs, f"git symbolic-ref {EXEC_BRANCH} {branch}")
            else:
                self._ssh_cmd(fs, f"git symbolic-ref -d {EXEC_BRANCH}", check=False)

            # checkout EXEC_HEAD and apply EXEC_MERGE on top of it without
            # committing
            head = EXEC_BRANCH if branch else EXEC_HEAD
            self._ssh_cmd(fs, f"git checkout {head}")
            merge_rev = scm.get_ref(EXEC_MERGE)
            self._ssh_cmd(fs, f"git stash apply {merge_rev}")

            if self._setup_script:
                self._init_setup_script(fs)

    @classmethod
    def _setup_script_path(cls, dvc_dir: str):
        return posixpath.join(
            dvc_dir,
            "tmp",
            cls.SETUP_SCRIPT_FILENAME,
        )

    def _init_setup_script(self, fs: "SSHFileSystem"):
        assert self._repo_abspath
        script_path = self._setup_script_path(
            posixpath.join(self._repo_abspath, self.dvc_dir)
        )
        assert self._setup_script
        fs.put_file(self._setup_script, script_path)

    def _ssh_cmd(self, sshfs, cmd, chdir=None, **kwargs):
        working_dir = chdir or self.root_dir
        return sshfs.fs.execute(f"cd {working_dir};{cmd}", **kwargs)

    def init_cache(self, repo: "Repo", rev: str, run_cache: bool = True):
        from dvc.repo.push import push

        with self.get_odb() as odb:
            push(
                repo,
                revs=[rev],
                run_cache=run_cache,
                odb=odb,
                include_imports=True,
            )

    def collect_cache(
        self, repo: "Repo", exp_ref: "ExpRefInfo", run_cache: bool = True
    ):
        """Collect DVC cache."""
        from dvc.repo.experiments.pull import _pull_cache

        _pull_cache(repo, exp_ref, run_cache=run_cache)

    @contextmanager
    def get_odb(self):
        from dvc.cachemgr import CacheManager, get_odb

        cache_path = posixpath.join(
            self._repo_abspath,
            self.dvc_dir,
            CacheManager.CACHE_DIR,
        )

        with self.sshfs() as fs:
            yield get_odb(fs, cache_path, **fs.config)

    def fetch_exps(self, *args, **kwargs) -> Iterable[str]:
        with self.sshfs() as fs:
            kwargs.update(self._git_client_args(fs))
            return super().fetch_exps(*args, **kwargs)

    @classmethod
    def reproduce(
        cls,
        info: "ExecutorInfo",
        rev: str,  # noqa: ARG003
        queue: Optional["Queue"] = None,  # noqa: ARG003
        infofile: Optional[str] = None,
        log_errors: bool = True,
        log_level: Optional[int] = None,
        copy_paths: Optional[List[str]] = None,  # noqa: ARG003
        message: Optional[str] = None,  # noqa: ARG003
        **kwargs,
    ) -> "ExecutorResult":
        """Reproduce an experiment on a remote machine over SSH.

        Internally uses 'dvc exp exec-run' over SSH.
        """
        import json
        import time
        from tempfile import TemporaryFile

        from asyncssh import ProcessError

        fs_factory: Optional[Callable] = kwargs.pop("fs_factory", None)
        if log_errors and log_level is not None:
            cls._set_log_level(log_level)

        with _sshfs(fs_factory) as fs:
            while not fs.exists("/var/log/dvc-machine-init.log"):
                logger.info("Waiting for dvc-machine startup script to complete...")
                time.sleep(5)
            logger.info("Reproducing experiment on '%s'", fs.fs_args.get("host"))
            with TemporaryFile(mode="w+", encoding="utf-8") as fobj:
                json.dump(info.asdict(), fobj)
                fobj.seek(0)
                fs.put_file(fobj, infofile)
            cmd = ["source ~/.profile"]
            script_path = cls._setup_script_path(info.dvc_dir)
            if fs.exists(posixpath.join(info.root_dir, script_path)):
                cmd.extend([f"pushd {info.root_dir}", f"source {script_path}", "popd"])
            exec_cmd = f"dvc exp exec-run --infofile {infofile}"
            if log_level is not None:
                if log_level <= logging.TRACE:  # type: ignore[attr-defined]
                    exec_cmd += " -vv"
                elif log_level <= logging.DEBUG:
                    exec_cmd += " -v"
            cmd.append(exec_cmd)
            try:
                sys.stdout.flush()
                sys.stderr.flush()
                stdout = os.dup(sys.stdout.fileno())
                stderr = os.dup(sys.stderr.fileno())
                fs.fs.execute("; ".join(cmd), stdout=stdout, stderr=stderr)
                with fs.open(infofile) as fobj:
                    result_info = ExecutorInfo.from_dict(json.load(fobj))
                if result_info.result_hash:
                    return result_info.result
            except ProcessError:
                pass
            return ExecutorResult(None, None, False)

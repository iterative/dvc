import copy
import os
from contextlib import contextmanager
import filecmp

from funcy import merge

from .local import DependencyLOCAL
from dvc.external_repo import cached_clone
from dvc.external_repo import external_repo
from dvc.exceptions import NotDvcRepoError
from dvc.exceptions import OutputNotFoundError
from dvc.exceptions import PathMissingError
from dvc.utils.fs import fs_copy
from dvc.path_info import PathInfo
from dvc.scm import SCM


class DependencyREPO(DependencyLOCAL):
    PARAM_REPO = "repo"
    PARAM_URL = "url"
    PARAM_REV = "rev"
    PARAM_REV_LOCK = "rev_lock"

    REPO_SCHEMA = {PARAM_URL: str, PARAM_REV: str, PARAM_REV_LOCK: str}

    def __init__(self, def_repo, stage, *args, **kwargs):
        self.def_repo = def_repo
        super().__init__(stage, *args, **kwargs)

    def _parse_path(self, remote, path):
        return None

    @property
    def is_in_repo(self):
        return False

    @property
    def repo_pair(self):
        d = self.def_repo
        return d[self.PARAM_URL], d[self.PARAM_REV_LOCK] or d[self.PARAM_REV]

    def __str__(self):
        return "{} ({})".format(self.def_path, self.def_repo[self.PARAM_URL])

    @contextmanager
    def _make_repo(self, **overrides):
        with external_repo(**merge(self.def_repo, overrides)) as repo:
            yield repo

    def _get_checksum_in_repo(self, repo):
        return repo.cache.local.get_checksum(
            PathInfo(os.path.join(repo.root_dir, self.def_path))
        )

    # fileinfo is a dictionary containing "checksum" and "path"
    def _get_fileinfo(self, updated=False):
        rev_lock = None
        if not updated:
            rev_lock = self.def_repo.get(self.PARAM_REV_LOCK)

        try:
            with self._make_repo(rev_lock=rev_lock) as repo:
                path = os.path.join(repo.root_dir, self.def_path)
                try:
                    checksum = repo.find_out_by_relpath(self.def_path).info[
                        "md5"
                    ]
                except OutputNotFoundError:
                    repo.state.load()
                    checksum = repo.cache.local.get_checksum(PathInfo(path))

                assert checksum

                return {"checksum": checksum, "path": path, "repo": repo}

        except NotDvcRepoError:
            repo_path = cached_clone(
                self.def_repo[self.PARAM_URL],
                rev=rev_lock or self.def_repo.get(self.PARAM_REV),
            )
            path = os.path.join(repo_path, self.def_path)
            return {"checksum": None, "path": path, "repo": None}

    @staticmethod
    def _paths_changed(current_path, updated_path):
        if not os.path.exists(current_path):
            raise FileNotFoundError(current_path)

        if not os.path.exists(updated_path):
            raise FileNotFoundError(updated_path)

        is_dir = os.path.isdir(current_path)

        assert is_dir == os.path.isdir(updated_path)

        if is_dir:
            comparison = filecmp.dircmp(current_path, updated_path)
            return not (
                comparison.left_only
                or comparison.right_only
                or comparison.diff_files
            )

        return not filecmp.cmp(current_path, updated_path, shallow=False)

    def _checkout_if_needed(self, fileinfo):
        if os.path.exists(fileinfo["path"]):
            return
        if "repo" not in fileinfo:
            return

        fileinfo["repo"].checkout([fileinfo["path"] + ".dvc"], recursive=True)

    def status(self):
        current_fileinfo = self._get_fileinfo(updated=False)
        updated_fileinfo = self._get_fileinfo(updated=True)

        if current_fileinfo["checksum"] and updated_fileinfo["checksum"]:
            has_changed = (
                current_fileinfo["checksum"] != updated_fileinfo["checksum"]
            )
        else:
            self._checkout_if_needed(current_fileinfo)
            self._checkout_if_needed(updated_fileinfo)
            has_changed = DependencyREPO._paths_changed(
                current_fileinfo["path"], updated_fileinfo["path"]
            )

        if has_changed:
            return {str(self): "update available"}

        return {}

    def save(self):
        pass

    def dumpd(self):
        return {self.PARAM_PATH: self.def_path, self.PARAM_REPO: self.def_repo}

    def fetch(self):
        with self._make_repo(
            cache_dir=self.repo.cache.local.cache_dir
        ) as repo:
            self.def_repo[self.PARAM_REV_LOCK] = repo.scm.get_rev()

            out = repo.find_out_by_relpath(self.def_path)
            with repo.state:
                repo.cloud.pull(out.get_used_cache())

        return out

    @staticmethod
    def _is_git_file(repo_dir, path):
        from dvc.repo import Repo

        if os.path.isabs(path):
            return False

        try:
            repo = Repo(repo_dir)
        except NotDvcRepoError:
            return True

        try:
            output = repo.find_out_by_relpath(path)
            return not output.use_cache
        except OutputNotFoundError:
            return True
        finally:
            repo.close()

    def _copy_if_git_file(self, to_path):
        src_path = self.def_path
        repo_dir = cached_clone(**self.def_repo)

        if not self._is_git_file(repo_dir, src_path):
            return False

        src_full_path = os.path.join(repo_dir, src_path)
        dst_full_path = os.path.abspath(to_path)
        fs_copy(src_full_path, dst_full_path)
        self.def_repo[self.PARAM_REV_LOCK] = SCM(repo_dir).get_rev()
        return True

    def download(self, to):
        try:
            if self._copy_if_git_file(to.fspath):
                return

            out = self.fetch()
            to.info = copy.copy(out.info)
            to.checkout()
        except (FileNotFoundError):
            raise PathMissingError(
                self.def_path, self.def_repo[self.PARAM_URL]
            )

    def update(self):
        with self._make_repo(rev_lock=None) as repo:
            self.def_repo[self.PARAM_REV_LOCK] = repo.scm.get_rev()

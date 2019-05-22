import os
import shutil
import tempfile
import logging

from dvc.exceptions import DvcException


logger = logging.getLogger(__name__)


class TempRepoException(DvcException):
    """Raise when temporary package repository has not properly created"""

    def __init__(self, temp_repo, msg, cause=None):
        m = "temp repository '{}' error: {}".format(temp_repo.addr, msg)
        super(TempRepoException, self).__init__(m, cause)


class TempGitRepo(object):
    GIT_DIR_TO_REMOVE = ".git"
    GIT_FILES_TO_REMOVE = [".gitignore", ".gitmodules"]
    SUFFIX = "tmp_DVC_mod"

    def __init__(self, addr, module_name, modules_dir):
        self.addr = addr
        self.modules_dir = modules_dir
        self._tmp_mod_prefix = module_name + "_" + self.SUFFIX + "_"
        self._reset_state()

    def _reset_state(self):
        self._set_state(None, [])

    def _set_state(self, cloned_tmp_dir, outs):
        from dvc.repo import Repo

        self.repo = Repo(cloned_tmp_dir) if cloned_tmp_dir else None

        self._cloned_tmp_dir = cloned_tmp_dir
        self.outs = outs
        self._moved_files = []

    def fetch(self, targets):
        for target in targets:
            try:
                self.repo.fetch(target)
            except Exception as ex:
                msg = "error in fetching data from {}: {}".format(
                    os.path.basename(target), ex
                )
                raise TempRepoException(self, msg)

    @property
    def is_state_set(self):
        return self._cloned_tmp_dir is not None

    def __enter__(self):
        if self.is_state_set:
            raise TempRepoException(self, "Git repo cloning duplication")

        module_temp_dir = None
        try:
            module_temp_dir = self._clone_to_temp_dir()
            self._remove_git_files_and_dirs(module_temp_dir)

            outputs = self._read_outputs(module_temp_dir)
            self._set_state(module_temp_dir, outputs)
        finally:
            self._clean_mod_temp_dir(module_temp_dir)

        return self

    def _clone_to_temp_dir(self):
        import git

        result = tempfile.mktemp(
            prefix=self._tmp_mod_prefix, dir=self.modules_dir
        )
        logger.debug(
            "Cloning git repository {} to temp dir {}".format(
                self.addr, result
            )
        )
        git.Repo.clone_from(self.addr, result, depth=1)
        return result

    def _remove_git_files_and_dirs(self, module_temp_dir):
        logger.debug(
            "Removing git meta files from {}: {} dif and {}".format(
                module_temp_dir,
                TempGitRepo.GIT_DIR_TO_REMOVE,
                ", ".join(TempGitRepo.GIT_FILES_TO_REMOVE),
            )
        )
        shutil.rmtree(
            os.path.join(module_temp_dir, TempGitRepo.GIT_DIR_TO_REMOVE)
        )
        for item in TempGitRepo.GIT_FILES_TO_REMOVE:
            fname = os.path.join(module_temp_dir, item)
            if os.path.exists(fname):
                os.remove(fname)

    def _clean_mod_temp_dir(self, module_temp_dir):
        if not self.is_state_set:
            if module_temp_dir and os.path.exists(module_temp_dir):
                shutil.rmtree(module_temp_dir, ignore_errors=True)

    def _read_outputs(self, module_temp_dir):
        from dvc.repo import Repo

        pkg_repo = Repo(module_temp_dir)
        stages = pkg_repo.stages()
        return [out for s in stages for out in s.outs]

    def persist_to(self, module_dir, parent_repo):
        if not self.is_state_set:
            raise TempRepoException(self, "cannot persist")

        tmp_repo_cache = self.repo.cache.local.url

        for prefix in os.listdir(tmp_repo_cache):
            if len(prefix) != 2:
                logger.warning(
                    "wrong dir format in cache {}: dir {}".format(
                        tmp_repo_cache, prefix
                    )
                )
            self._move_all_cache_files(parent_repo, prefix, tmp_repo_cache)

        shutil.move(self._cloned_tmp_dir, module_dir)
        self._reset_state()

    @staticmethod
    def _move_all_cache_files(parent_repo, prefix, tmp_repo_cache):
        obj_name = os.path.join(tmp_repo_cache, prefix)
        for suffix in os.listdir(obj_name):
            src_path = os.path.join(tmp_repo_cache, prefix, suffix)
            pre = os.path.join(parent_repo.cache.local.url, prefix)
            if not os.path.exists(pre):
                os.mkdir(pre)
            dest = os.path.join(pre, suffix)
            shutil.move(src_path, dest)

    def __exit__(self, exc_type, exc_value, traceback):
        if self.is_state_set:
            shutil.rmtree(self._cloned_tmp_dir)
            self._reset_state()
        pass

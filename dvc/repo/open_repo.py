import os
import tempfile
import threading
from typing import TYPE_CHECKING, Optional

from funcy import retry, wrap_with

from dvc.exceptions import NotDvcRepoError
from dvc.log import logger
from dvc.repo import Repo
from dvc.scm import CloneError, map_scm_exception
from dvc.utils import relpath

if TYPE_CHECKING:
    from dvc.scm import Git

logger = logger.getChild(__name__)


@map_scm_exception()
def _external_repo(url, rev: Optional[str] = None, **kwargs) -> "Repo":
    logger.debug("Creating external repo %s@%s", url, rev)
    path = _cached_clone(url, rev)
    # Local HEAD points to the tip of whatever branch we first cloned from
    # (which may not be the default branch), use origin/HEAD here to get
    # the tip of the default branch
    rev = rev or "refs/remotes/origin/HEAD"

    config = _get_remote_config(url) if os.path.isdir(url) else {}
    config.update({"cache": {"dir": _get_cache_dir(url)}})
    config.update(kwargs.pop("config", None) or {})

    main_root = "/"
    repo_kwargs = dict(
        root_dir=path,
        url=url,
        config=config,
        repo_factory=erepo_factory(url, main_root, {"cache": config["cache"]}),
        rev=rev,
        **kwargs,
    )

    return Repo(**repo_kwargs)


def open_repo(url, *args, **kwargs):
    if url is None:
        url = os.getcwd()

    if os.path.exists(url):
        url = os.path.abspath(url)
        try:
            config = _get_remote_config(url)
            config.update(kwargs.get("config") or {})
            kwargs["config"] = config
            return Repo(url, *args, **kwargs)
        except NotDvcRepoError:
            pass  # fallthrough to _external_repo

    return _external_repo(url, *args, **kwargs)


def erepo_factory(url, root_dir, cache_config):
    from dvc.fs import localfs

    def make_repo(path, fs=None, **_kwargs):
        _config = cache_config.copy()
        if os.path.isdir(url):
            fs = fs or localfs
            repo_path = os.path.join(url, *fs.relparts(path, root_dir))
            _config.update(_get_remote_config(repo_path))
        return Repo(path, fs=fs, config=_config, **_kwargs)

    return make_repo


CLONES: dict[str, tuple[str, bool]] = {}
CACHE_DIRS: dict[str, str] = {}


@wrap_with(threading.Lock())
def _get_cache_dir(url):
    try:
        cache_dir = CACHE_DIRS[url]
    except KeyError:
        cache_dir = CACHE_DIRS[url] = tempfile.mkdtemp("dvc-cache")
    return cache_dir


def clean_repos():
    # Outside code should not see cache while we are removing
    paths = [path for path, _ in CLONES.values()] + list(CACHE_DIRS.values())
    CLONES.clear()
    CACHE_DIRS.clear()

    for path in paths:
        _remove(path)


def _get_remote_config(url):
    try:
        repo = Repo(url, uninitialized=True)
    except NotDvcRepoError:
        return {}

    try:
        name = repo.config["core"].get("remote")
        if not name:
            # Fill the empty upstream entry with a new remote pointing to the
            # original repo's cache location.
            name = "auto-generated-upstream"
            try:
                local_cache_dir = repo.cache.local_cache_dir
            except AttributeError:
                # if the `.dvc` dir is missing, we get an AttributeError
                return {}
            else:
                return {
                    "core": {"remote": name},
                    "remote": {name: {"url": local_cache_dir}},
                }

        # Use original remote to make sure that we are using correct url,
        # credential paths, etc if they are relative to the config location.
        return {"remote": {name: repo.config["remote"][name]}}
    finally:
        repo.close()


def _cached_clone(url, rev):
    """Clone an external git repo to a temporary directory.

    Returns the path to a local temporary directory with the specified
    revision checked out.
    """
    from shutil import copytree

    # even if we have already cloned this repo, we may need to
    # fetch/fast-forward to get specified rev
    clone_path, shallow = _clone_default_branch(url, rev)

    if url in CLONES:
        return CLONES[url][0]

    # Copy to a new dir to keep the clone clean
    repo_path = tempfile.mkdtemp("dvc-erepo")
    logger.debug("erepo: making a copy of %s clone", url)
    copytree(clone_path, repo_path)

    CLONES[url] = (repo_path, shallow)
    return repo_path


@wrap_with(threading.Lock())
def _clone_default_branch(url, rev):
    """Get or create a clean clone of the url.

    The cloned is reactualized with git pull unless rev is a known sha.
    """
    from dvc.scm import Git

    clone_path, shallow = CLONES.get(url) or (None, False)

    git = None
    try:
        if clone_path:
            git = Git(clone_path)
            # Do not pull for known shas, branches and tags might move
            if not Git.is_sha(rev) or not git.has_rev(rev):
                if shallow:
                    # If we are missing a rev in a shallow clone, fallback to
                    # a full (unshallowed) clone. Since fetching specific rev
                    # SHAs is only available in certain git versions, if we
                    # have need to reference multiple specific revs for a
                    # given repo URL it is easier/safer for us to work with
                    # full clones in this case.
                    logger.debug("erepo: unshallowing clone for '%s'", url)
                    _pull(git, unshallow=True)
                    shallow = False
                    CLONES[url] = (clone_path, shallow)
                else:
                    logger.debug("erepo: git pull '%s'", url)
                    _pull(git)
        else:
            from dvc.scm import clone

            logger.debug("erepo: git clone '%s' to a temporary dir", url)
            clone_path = tempfile.mkdtemp("dvc-clone")
            if rev and not Git.is_sha(rev):
                # If rev is a tag or branch name try shallow clone first

                try:
                    git = clone(url, clone_path, shallow_branch=rev)
                    shallow = os.path.exists(
                        os.path.join(clone_path, Git.GIT_DIR, "shallow")
                    )
                    if shallow:
                        logger.debug("erepo: using shallow clone for branch '%s'", rev)
                except CloneError:
                    git_dir = os.path.join(clone_path, ".git")
                    if os.path.exists(git_dir):
                        _remove(git_dir)
            if not git:
                git = clone(url, clone_path)
                shallow = False
            CLONES[url] = (clone_path, shallow)
    finally:
        if git:
            git.close()

    return clone_path, shallow


def _pull(git: "Git", unshallow: bool = False):
    from dvc.repo.experiments.utils import fetch_all_exps

    git.fetch(unshallow=unshallow)
    _merge_upstream(git)
    fetch_all_exps(git, "origin")


def _merge_upstream(git: "Git"):
    from scmrepo.exceptions import SCMError

    try:
        branch = git.active_branch()
        upstream = f"refs/remotes/origin/{branch}"
        if git.get_ref(upstream):
            git.merge(upstream)
    except SCMError:
        pass


def _remove(path):
    from dvc.utils.fs import remove

    if os.name == "nt":
        # git.exe may hang for a while not permitting to remove temp dir
        os_retry = retry(5, errors=OSError, timeout=0.1)
        try:
            os_retry(remove)(path)
        except PermissionError:
            logger.warning("Failed to remove '%s'", relpath(path), exc_info=True)
    else:
        remove(path)

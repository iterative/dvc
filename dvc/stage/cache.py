import logging
import os
import tempfile
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from funcy import cached_property, first

from dvc.exceptions import DvcException
from dvc.utils import dict_sha256, relpath

if TYPE_CHECKING:
    from dvc.objects.db.base import ObjectDB

logger = logging.getLogger(__name__)


class RunCacheNotFoundError(DvcException):
    def __init__(self, stage):
        super().__init__(f"No run-cache for {stage.addressing}")


def _get_cache_hash(cache, key=False):
    from dvc.data.meta import Meta

    if key:
        cache["outs"] = [out["path"] for out in cache.get("outs", [])]
    return dict_sha256(cache, exclude=[Meta.PARAM_SIZE, Meta.PARAM_NFILES])


def _can_hash(stage):
    if stage.is_callback or stage.always_changed:
        return False

    if not all([stage.cmd, stage.deps, stage.outs]):
        return False

    for dep in stage.deps:
        if not (dep.scheme == "local" and dep.def_path and dep.get_hash()):
            return False

    for out in stage.outs:
        if out.scheme != "local" or not out.def_path or out.persist:
            return False

    return True


def _get_stage_hash(stage):
    from .serialize import to_single_stage_lockfile

    assert _can_hash(stage)
    return _get_cache_hash(to_single_stage_lockfile(stage), key=True)


class StageCache:
    def __init__(self, repo):
        self.repo = repo

    @cached_property
    def cache_dir(self):
        return os.path.join(self.repo.odb.local.cache_dir, "runs")

    def _get_cache_dir(self, key):
        return os.path.join(self.cache_dir, key[:2], key)

    def _get_cache_path(self, key, value):
        return os.path.join(self._get_cache_dir(key), value)

    def _load_cache(self, key, value):
        from voluptuous import Invalid

        from dvc.schema import COMPILED_LOCK_FILE_STAGE_SCHEMA
        from dvc.utils.serialize import YAMLFileCorruptedError, load_yaml

        path = self._get_cache_path(key, value)

        try:
            return COMPILED_LOCK_FILE_STAGE_SCHEMA(load_yaml(path))
        except FileNotFoundError:
            return None
        except (YAMLFileCorruptedError, Invalid):
            logger.warning("corrupted cache file '%s'.", relpath(path))
            os.unlink(path)
            return None

    def _load(self, stage):
        key = _get_stage_hash(stage)
        if not key:
            return None

        cache_dir = self._get_cache_dir(key)
        if not os.path.exists(cache_dir):
            return None

        for value in os.listdir(cache_dir):
            cache = self._load_cache(key, value)
            if cache:
                return cache

        return None

    def _create_stage(self, cache, wdir=None):
        from . import PipelineStage, create_stage
        from .loader import StageLoader

        stage = create_stage(
            PipelineStage,
            repo=self.repo,
            path="dvc.yaml",
            cmd=cache["cmd"],
            wdir=wdir,
            outs=[out["path"] for out in cache["outs"]],
            external=True,
        )
        StageLoader.fill_from_lock(stage, cache)
        return stage

    @contextmanager
    def _cache_type_copy(self):
        cache_types = self.repo.odb.local.cache_types
        self.repo.odb.local.cache_types = ["copy"]
        try:
            yield
        finally:
            self.repo.odb.local.cache_types = cache_types

    def _uncached_outs(self, stage, cache):
        # NOTE: using temporary stage to avoid accidentally modifying original
        # stage and to workaround `commit/checkout` not working for uncached
        # outputs.
        cached_stage = self._create_stage(cache, wdir=stage.wdir)

        outs_no_cache = [
            out.def_path for out in stage.outs if not out.use_cache
        ]

        # NOTE: using copy link to make it look like a git-tracked file
        with self._cache_type_copy():
            for out in cached_stage.outs:
                if out.def_path in outs_no_cache:
                    yield out

    def save(self, stage):
        from .serialize import to_single_stage_lockfile

        if not _can_hash(stage):
            return

        cache_key = _get_stage_hash(stage)
        cache = to_single_stage_lockfile(stage)
        cache_value = _get_cache_hash(cache)

        existing_cache = self._load_cache(cache_key, cache_value)
        cache = existing_cache or cache

        for out in self._uncached_outs(stage, cache):
            out.commit()

        if existing_cache:
            return

        from dvc.schema import COMPILED_LOCK_FILE_STAGE_SCHEMA
        from dvc.utils.serialize import dump_yaml

        # sanity check
        COMPILED_LOCK_FILE_STAGE_SCHEMA(cache)

        path = self._get_cache_path(cache_key, cache_value)
        parent = self.repo.odb.local.fs.path.parent(path)
        self.repo.odb.local.makedirs(parent)
        tmp = tempfile.NamedTemporaryFile(delete=False, dir=parent).name
        assert os.path.exists(parent)
        assert os.path.isdir(parent)
        dump_yaml(tmp, cache)
        self.repo.odb.local.move(tmp, path)

    def restore(self, stage, run_cache=True, pull=False):
        from .serialize import to_single_stage_lockfile

        if not _can_hash(stage):
            raise RunCacheNotFoundError(stage)

        if (
            not stage.changed_stage()
            and stage.deps_cached()
            and all(bool(out.hash_info) for out in stage.outs)
        ):
            cache = to_single_stage_lockfile(stage)
        else:
            if not run_cache:  # backward compatibility
                raise RunCacheNotFoundError(stage)
            stage.save_deps()
            cache = self._load(stage)
            if not cache:
                raise RunCacheNotFoundError(stage)

        cached_stage = self._create_stage(cache, wdir=stage.wdir)

        if pull:
            for objs in cached_stage.get_used_objs().values():
                self.repo.cloud.pull(objs)

        if not cached_stage.outs_cached():
            raise RunCacheNotFoundError(stage)

        logger.info(
            "Stage '%s' is cached - skipping run, checking out outputs",
            stage.addressing,
        )
        cached_stage.checkout()

    @staticmethod
    def _transfer(func, from_remote, to_remote):
        ret = []

        runs = from_remote.fs.path.join(from_remote.fs_path, "runs")
        if not from_remote.fs.exists(runs):
            return []

        from_path = from_remote.fs.path
        for src in from_remote.fs.find(runs):
            rel = from_path.relpath(src, from_remote.fs_path)
            dst = to_remote.fs.path.join(to_remote.fs_path, rel)
            key = to_remote.fs.path.parent(dst)
            # check if any build cache already exists for this key
            # TODO: check if MaxKeys=1 or something like that applies
            # or otherwise this will take a lot of time!
            if to_remote.fs.exists(key) and first(to_remote.fs.find(key)):
                continue
            func(src, dst)
            ret.append(
                (from_path.name(from_path.parent(src)), from_path.name(src))
            )

        return ret

    def push(self, remote: Optional[str], odb: Optional["ObjectDB"] = None):
        from dvc.data.transfer import _log_exceptions

        if odb is None:
            odb = self.repo.cloud.get_remote_odb(remote)
        return self._transfer(
            _log_exceptions(odb.fs.upload),
            self.repo.odb.local,
            odb,
        )

    def pull(self, remote: Optional[str]):
        from dvc.data.transfer import _log_exceptions

        odb = self.repo.cloud.get_remote_odb(remote)
        return self._transfer(
            _log_exceptions(odb.fs.download),
            odb,
            self.repo.odb.local,
        )

    def get_used_objs(self, used_run_cache, *args, **kwargs):
        """Return used cache for the specified run-cached stages."""
        from collections import defaultdict

        used_objs = defaultdict(set)
        for key, value in used_run_cache:
            entry = self._load_cache(key, value)
            if not entry:
                continue
            stage = self._create_stage(entry)
            for odb, objs in stage.get_used_objs(*args, **kwargs).items():
                used_objs[odb].update(objs)
        return used_objs

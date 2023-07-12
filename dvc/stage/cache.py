import logging
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, List, Optional, Tuple

from funcy import first

from dvc import fs
from dvc.exceptions import CollectCacheError, DvcException
from dvc.utils import dict_sha256, relpath

if TYPE_CHECKING:
    from dvc_objects.db import ObjectDB

logger = logging.getLogger(__name__)


class RunCacheNotFoundError(DvcException):
    def __init__(self, stage):
        super().__init__(f"No run-cache for {stage.addressing}")


class RunCacheNotSupported(DvcException):
    pass


def _get_cache_hash(cache, key=False):
    from dvc_data.hashfile.meta import Meta

    if key:
        cache["outs"] = [out["path"] for out in cache.get("outs", [])]
    return dict_sha256(cache, exclude=[Meta.PARAM_SIZE, Meta.PARAM_NFILES])


def _can_hash(stage):
    if stage.is_callback or stage.always_changed:
        return False

    if not all([stage.cmd, stage.deps, stage.outs]):
        return False

    for dep in stage.deps:
        if not (dep.protocol == "local" and dep.def_path and dep.get_hash()):
            return False

    for out in stage.outs:
        if (
            out.protocol != "local"
            or not out.def_path
            or out.persist
            or not out.is_in_repo
        ):
            return False

    return True


def _get_stage_hash(stage):
    from .serialize import to_single_stage_lockfile

    assert _can_hash(stage)
    return _get_cache_hash(to_single_stage_lockfile(stage), key=True)


class StageCache:
    def __init__(self, repo):
        self.repo = repo
        self.cache_dir = os.path.join(self.repo.cache.legacy.path, "runs")

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
        )
        StageLoader.fill_from_lock(stage, cache)
        return stage

    @contextmanager
    def _cache_type_copy(self):
        cache_types = self.repo.cache.local.cache_types
        legacy_cache_types = self.repo.cache.legacy.cache_types
        self.repo.cache.local.cache_types = ["copy"]
        self.repo.cache.legacy.cache_types = ["copy"]
        try:
            yield
        finally:
            self.repo.cache.local.cache_types = cache_types
            self.repo.cache.legacy.cache_types = legacy_cache_types

    def _uncached_outs(self, stage, cache):
        # NOTE: using temporary stage to avoid accidentally modifying original
        # stage and to workaround `commit/checkout` not working for uncached
        # outputs.
        cached_stage = self._create_stage(cache, wdir=stage.wdir)

        outs_no_cache = [out.def_path for out in stage.outs if not out.use_cache]

        # NOTE: using copy link to make it look like a git-tracked file
        with self._cache_type_copy():
            for out in cached_stage.outs:
                if out.def_path in outs_no_cache and out.is_in_repo:
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
        local_fs = self.repo.cache.legacy.fs
        parent = local_fs.path.parent(path)
        self.repo.cache.legacy.makedirs(parent)
        tmp = local_fs.path.join(parent, fs.utils.tmp_fname())
        assert os.path.exists(parent)
        assert os.path.isdir(parent)
        dump_yaml(tmp, cache)
        self.repo.cache.legacy.move(tmp, path)

    def restore(self, stage, run_cache=True, pull=False, dry=False):  # noqa: C901
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
            if not dry:
                stage.save_deps()
            cache = self._load(stage)
            if not cache:
                raise RunCacheNotFoundError(stage)

        cached_stage = self._create_stage(cache, wdir=stage.wdir)

        if pull and not dry:
            try:
                for objs in cached_stage.get_used_objs().values():
                    self.repo.cloud.pull(objs)
            except CollectCacheError as exc:
                raise RunCacheNotFoundError(stage) from exc

        if not cached_stage.outs_cached():
            raise RunCacheNotFoundError(stage)

        logger.info(
            "Stage '%s' is cached - skipping run, checking out outputs",
            stage.addressing,
        )
        if not dry:
            cached_stage.checkout()

    def transfer(self, from_odb, to_odb):
        from dvc.fs import HTTPFileSystem, LocalFileSystem
        from dvc.fs.callbacks import Callback

        from_fs = from_odb.fs
        to_fs = to_odb.fs
        func = fs.generic.log_exceptions(fs.generic.copy)
        runs = from_fs.path.join(from_odb.path, "runs")

        http_odb = next(
            (odb for odb in (from_odb, to_odb) if isinstance(odb.fs, HTTPFileSystem)),
            None,
        )
        if http_odb:
            path = http_odb.path
            message = f"run-cache is not supported for http filesystem: {path}"
            raise RunCacheNotSupported(message)

        ret: List[Tuple[str, str]] = []
        if not from_fs.exists(runs):
            return ret

        for src in from_fs.find(runs):
            rel = from_fs.path.relpath(src, from_odb.path)
            if not isinstance(to_fs, LocalFileSystem):
                rel = from_fs.path.as_posix(rel)

            dst = to_fs.path.join(to_odb.path, rel)
            key = to_fs.path.parent(dst)
            # check if any build cache already exists for this key
            # TODO: check if MaxKeys=1 or something like that applies
            # or otherwise this will take a lot of time!
            if to_fs.exists(key) and first(to_fs.find(key)):
                continue

            src_name = from_fs.path.name(src)
            parent_name = from_fs.path.name(from_fs.path.parent(src))
            with Callback.as_tqdm_callback(
                desc=src_name,
                bytes=True,
            ) as cb:
                func(from_fs, src, to_fs, dst, callback=cb)
            ret.append((parent_name, src_name))
        return ret

    def push(self, remote: Optional[str], odb: Optional["ObjectDB"] = None):
        dest_odb = odb or self.repo.cloud.get_remote_odb(
            remote, "push --run-cache", hash_name="md5-dos2unix"
        )
        return self.transfer(self.repo.cache.legacy, dest_odb)

    def pull(self, remote: Optional[str], odb: Optional["ObjectDB"] = None):
        odb = odb or self.repo.cloud.get_remote_odb(
            remote, "fetch --run-cache", hash_name="md5-dos2unix"
        )
        return self.transfer(odb, self.repo.cache.legacy)

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

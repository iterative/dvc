import os
import re
import logging

from funcy import project

import dvc.prompt as prompt

from voluptuous import MultipleInvalid

from dvc import dependency, output
from dvc.exceptions import DvcException
from dvc.stage.exceptions import (
    StageFileBadNameError,
    StageFileDoesNotExistError,
    StageFileIsNotDvcFileError,
    StageFileFormatError,
    StageFileAlreadyExistsError,
)
from dvc.utils import relpath
from dvc.utils.collections import apply_diff
from dvc.utils.stage import (
    parse_stage_for_update,
    dump_stage_file,
    parse_stage,
)

logger = logging.getLogger(__name__)

DVC_FILE = "Dvcfile"
DVC_FILE_SUFFIX = ".dvc"
TAG_REGEX = r"^(?P<path>.*)@(?P<tag>[^\\/@:]*)$"


class MultiStageFileLoadError(DvcException):
    def __init__(self):
        super().__init__("Cannot load multi-stage file.")


def _serialize_stage(stage):
    outs_bucket = {}
    for o in stage.outs:
        bucket_key = ["metrics"] if o.metric else ["outs"]

        if not o.metric and o.persist:
            bucket_key += ["persist"]
        if not o.use_cache:
            bucket_key += ["no_cache"]
        key = "_".join(bucket_key)
        outs_bucket[key] = outs_bucket.get(key, []) + [o.def_path]

    return {
        stage.name: {
            key: value
            for key, value in {
                stage.PARAM_CMD: stage.cmd,
                stage.PARAM_WDIR: stage.resolve_wdir(),
                stage.PARAM_DEPS: [d.def_path for d in stage.deps],
                **outs_bucket,
                stage.PARAM_LOCKED: stage.locked,
                stage.PARAM_ALWAYS_CHANGED: stage.always_changed,
            }.items()
            if value
        }
    }


class Dvcfile:
    def __init__(self, repo, path):
        self.repo = repo
        self.path, self.tag = self._get_path_tag(path)

    def __repr__(self):
        return "{}: {}".format(DVC_FILE, self.path)

    @classmethod
    def is_valid_filename(cls, path):
        return (
            path.endswith(DVC_FILE_SUFFIX)
            or os.path.basename(path) == DVC_FILE
        )

    @classmethod
    def is_stage_file(cls, path):
        return os.path.isfile(path) and cls.is_valid_filename(path)

    @classmethod
    def check_dvc_filename(cls, path):
        if not cls.is_valid_filename(path):
            raise StageFileBadNameError(
                "bad DVC-file name '{}'. DVC-files should be named "
                "'Dvcfile' or have a '.dvc' suffix (e.g. '{}.dvc').".format(
                    relpath(path), os.path.basename(path)
                )
            )

    def exists(self):
        return self.repo.tree.exists(self.path)

    def check_file_exists(self):
        if not self.exists():
            raise StageFileDoesNotExistError(self.path)

    def check_isfile(self):
        if not self.repo.tree.isfile(self.path):
            raise StageFileIsNotDvcFileError(self.path)

    @staticmethod
    def _get_path_tag(s):
        regex = re.compile(TAG_REGEX)
        match = regex.match(s)
        if not match:
            return s, None
        return match.group("path"), match.group("tag")

    @property
    def lockfile(self):
        return os.path.splitext(self.path)[0] + ".lock"

    def dump(self, stage, update_dvcfile=False):
        """Dumps given stage appropriately in the dvcfile."""
        if not hasattr(stage, "name"):
            self.dump_single_stage(stage)
            return

        self.dump_lockfile(stage)
        if update_dvcfile and not stage.is_data_source:
            self.dump_multistage_dvcfile(stage)

        from .stage import Stage, create_stage

        for out in stage.outs:
            if not out.use_cache:
                continue
            s = create_stage(
                Stage,
                stage.repo,
                os.path.join(stage.wdir, out.def_path + DVC_FILE_SUFFIX),
                wdir=stage.wdir,
            )
            s.outs = [out]
            s.md5 = s._compute_md5()
            Dvcfile(s.repo, s.path).dump_single_stage(s)

    def dump_lockfile(self, stage):
        from . import lockfile

        lockfile.dump(self.repo, self.lockfile, stage)
        self.repo.scm.track_file(relpath(self.lockfile))

    def dump_multistage_dvcfile(self, stage):
        from dvc.utils.stage import parse_stage_for_update, dump_stage_file
        from dvc.schema import COMPILED_MULTI_STAGE_SCHEMA

        path = self.path
        if not os.path.exists(path):
            open(path, "w+").close()

        with open(path, "r") as fd:
            data = parse_stage_for_update(fd.read(), path)

        if not self.is_multi_stage(data):
            raise MultiStageFileLoadError

        # handle this in Stage::dumpd()
        data["stages"] = data.get("stages", {})
        data["stages"].update(_serialize_stage(stage))

        dump_stage_file(path, COMPILED_MULTI_STAGE_SCHEMA(data))
        self.repo.scm.track_file(relpath(path))

    def dump_single_stage(self, stage):
        self.check_dvc_filename(self.path)

        logger.debug(
            "Saving information to '{file}'.".format(file=relpath(self.path))
        )
        state = stage.dumpd()

        # When we load a stage we parse yaml with a fast parser, which strips
        # off all the comments and formatting. To retain those on update we do
        # a trick here:
        # - reparse the same yaml text with a slow but smart ruamel yaml parser
        # - apply changes to a returned structure
        # - serialize it
        if stage._stage_text is not None:
            saved_state = parse_stage_for_update(stage._stage_text, self.path)
            # Stage doesn't work with meta in any way, so .dumpd() doesn't
            # have it. We simply copy it over.
            if "meta" in saved_state:
                state["meta"] = saved_state["meta"]
            apply_diff(state, saved_state)
            state = saved_state

        dump_stage_file(self.path, state)

        self.repo.scm.track_file(relpath(self.path))

    def _load(self):
        # it raises the proper exceptions by priority:
        # 1. when the file doesn't exists
        # 2. filename is not a DVC-file
        # 3. path doesn't represent a regular file
        self.check_file_exists()
        self.check_dvc_filename(self.path)
        self.check_isfile()

        with self.repo.tree.open(self.path) as fd:
            stage_text = fd.read()
        d = parse_stage(stage_text, self.path)
        return d, stage_text

    def load_one(self, target=None):
        data, raw = self._load()
        if not self.is_multi_stage(data):
            if target:
                logger.warning(
                    "Ignoring target name '%s' as it's a single stage file.",
                    target,
                )
            return self._load_single_stage(data, raw)

        if not target:
            raise DvcException(
                "No target provided for multi-stage file '{}'.".format(
                    self.path
                )
            )

        if not self.has_stage(name=target, data=data):
            raise DvcException(
                "Target '{}' does not exist "
                "inside '{}' multi-stage file.".format(target, self.path)
            )

        stages = self._load_multi_stage(
            {"stages": {target: self._get_stage_data(target, data)}}
        )
        assert stages
        return stages[0]

    @staticmethod
    def _get_stage_data(name, data):
        return data.get("stages", {}).get(name)

    def has_stage(self, name, data=None):
        if not data:
            data, _ = self._load()
        return bool(self._get_stage_data(name, data))

    def load(self):
        """Loads single stage."""
        data, raw = self._load()
        if not self.is_multi_stage(data):
            return self._load_single_stage(data, raw)

        raise MultiStageFileLoadError

    def load_all(self):
        data, raw = self._load()
        return (
            [self._load_single_stage(data, raw)]
            if not self.is_multi_stage(data)
            else self._load_multi_stage(data)
        )

    def _load_single_stage(self, d: dict, stage_text: str):
        from dvc.stage import Stage, loads_from

        path = os.path.abspath(self.path)
        wdir = os.path.abspath(
            os.path.join(os.path.dirname(path), d.get(Stage.PARAM_WDIR, "."))
        )
        stage = loads_from(Stage, self.repo, path, wdir, d)
        stage._stage_text, stage.tag = stage_text, self.tag
        stage.deps = dependency.loadd_from(
            stage, d.get(Stage.PARAM_DEPS) or []
        )
        stage.outs = output.loadd_from(stage, d.get(Stage.PARAM_OUTS) or [])

        return stage

    def load_multi(self):
        data, _ = self._load()
        if self.is_multi_stage(data):
            return self._load_multi_stage(data)
        raise DvcException(
            "Cannot load multiple stages from single stage file."
        )

    def _load_multi_stage(self, data):
        from . import lockfile
        from .stage import PipelineStage, Stage, loads_from

        stages = []
        path = os.path.abspath(self.path)
        lock_data = lockfile.load(self.repo, self.lockfile)
        for stage_name, d in data.get("stages", {}).items():
            lock_stage_data = lock_data.get(stage_name, {})
            wdir = os.path.abspath(
                os.path.join(
                    os.path.dirname(path), d.get(Stage.PARAM_WDIR, ".")
                )
            )
            stage = loads_from(PipelineStage, self.repo, path, wdir, d)
            stage.name = stage_name
            stage.cmd_changed = lock_stage_data.get(Stage.PARAM_CMD) != d.get(
                Stage.PARAM_CMD
            )

            stage._fill_stage_dependencies(**project(d, ["deps"]))
            stage._fill_stage_outputs(**d)
            stages.append(stage)

            for dep in stage.deps:
                dep.info[dep.remote.PARAM_CHECKSUM] = lock_stage_data.get(
                    Stage.PARAM_DEPS, {}
                ).get(dep.def_path)

            if stage.cmd_changed:
                continue

            for out in stage.outs:
                out.info[out.remote.PARAM_CHECKSUM] = lock_stage_data.get(
                    Stage.PARAM_OUTS, {}
                ).get(out.def_path)

        return stages

    @staticmethod
    def validate_single_stage(d, fname=None):
        from dvc.schema import COMPILED_SINGLE_STAGE_SCHEMA

        try:
            COMPILED_SINGLE_STAGE_SCHEMA(d)
        except MultipleInvalid as exc:
            raise StageFileFormatError(fname, exc)

    @staticmethod
    def validate_multi_stage(d, fname=None):
        from dvc.schema import COMPILED_MULTI_STAGE_SCHEMA

        try:
            COMPILED_MULTI_STAGE_SCHEMA(d)
        except MultipleInvalid as exc:
            raise StageFileFormatError(fname, exc)

    @staticmethod
    def validate(d, fname=None):
        Dvcfile.validate_single_stage(d, fname)

    def is_multi_stage(self, d=None):
        # TODO: maybe the following heuristics is enough?
        if d is None:
            d = self._load()[0]
        check_multi_stage = d.get("stages") or not d
        exc = None
        if check_multi_stage:
            try:
                self.validate_multi_stage(d, self.path)
                return True
            except StageFileFormatError as _exc:
                exc = _exc

        try:
            self.validate_single_stage(d, self.path)
            return False
        except StageFileFormatError:
            if check_multi_stage:
                raise exc

        self.validate_multi_stage(d, self.path)
        return True

    def overwrite_with_prompt(self, force=False):
        if not self.exists():
            return

        msg = (
            "'{}' already exists. Do you wish to run the command and "
            "overwrite it?".format(relpath(self.path))
        )
        if not (force or prompt.confirm(msg)):
            raise StageFileAlreadyExistsError(self.path)

        os.unlink(self.path)

import os
import re
import logging

import dvc.prompt as prompt

from voluptuous import MultipleInvalid
from dvc import serialize
from dvc.exceptions import DvcException
from dvc.loader import SingleStageLoader, StageLoader
from dvc.schema import (
    COMPILED_SINGLE_STAGE_SCHEMA,
    COMPILED_MULTI_STAGE_SCHEMA,
)
from dvc.stage.exceptions import (
    StageFileBadNameError,
    StageFileDoesNotExistError,
    StageFileIsNotDvcFileError,
    StageFileFormatError,
    StageFileAlreadyExistsError,
)
from dvc.utils import relpath
from dvc.utils.stage import (
    dump_stage_file,
    parse_stage,
    parse_stage_for_update,
)

logger = logging.getLogger(__name__)

DVC_FILE = "Dvcfile"
DVC_FILE_SUFFIX = ".dvc"
TAG_REGEX = r"^(?P<path>.*)@(?P<tag>[^\\/@:]*)$"


class MultiStageFileLoadError(DvcException):
    def __init__(self, file):
        super().__init__("Cannot load multi-stage file: '{}'".format(file))


class Dvcfile:
    def __init__(self, repo, path):
        self.repo = repo
        self.path, self.tag = self._get_path_tag(path)

    def __repr__(self):
        return "{}: {}".format(
            DVC_FILE, relpath(self.path, self.repo.root_dir)
        )

    def __str__(self):
        return "{}: {}".format(DVC_FILE, self.relpath)

    @property
    def relpath(self):
        return relpath(self.path)

    @property
    def stage(self):
        data, raw = self._load()
        if not self.is_multi_stage(data):
            return SingleStageLoader.load_stage(self, data, raw)
        raise MultiStageFileLoadError(self.path)

    @property
    def lockfile(self):
        return os.path.splitext(self.path)[0] + ".lock"

    @property
    def stages(self):
        from . import lockfile

        data, raw = self._load()
        if self.is_multi_stage(data):
            lockfile_data = lockfile.load(self.repo, self.lockfile)
            return StageLoader(self, data.get("stages", {}), lockfile_data)
        return SingleStageLoader(self, data, raw)

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

    def dump(self, stage, update_dvcfile=False):
        """Dumps given stage appropriately in the dvcfile."""
        from dvc.stage import create_stage, PipelineStage, Stage

        if not isinstance(stage, PipelineStage):
            self.dump_single_stage(stage)
            return

        self.dump_lockfile(stage)
        if update_dvcfile and not stage.is_data_source:
            self.dump_multistage_dvcfile(stage)

        for out in filter(lambda o: o.use_cache, stage.outs):
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

        lockfile.dump(self.repo, self.lockfile, serialize.to_lockfile(stage))
        self.repo.scm.track_file(relpath(self.lockfile))

    def dump_multistage_dvcfile(self, stage):
        data = {}
        if self.exists():
            with open(self.path, "r") as fd:
                data = parse_stage_for_update(fd.read(), self.path)
            if not self.is_multi_stage(data):
                raise MultiStageFileLoadError(self.path)
        else:
            open(self.path, "w+").close()

        data["stages"] = data.get("stages", {})
        data["stages"].update(serialize.to_dvcfile(stage))

        dump_stage_file(self.path, COMPILED_MULTI_STAGE_SCHEMA(data))
        self.repo.scm.track_file(relpath(self.path))

    def dump_single_stage(self, stage):
        self.check_dvc_filename(self.path)

        logger.debug(
            "Saving information to '{file}'.".format(file=relpath(self.path))
        )

        dump_stage_file(self.path, serialize.to_single_stage_file(stage))
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

    @staticmethod
    def validate_single_stage(d, fname=None):
        Dvcfile._validate(COMPILED_SINGLE_STAGE_SCHEMA, d, fname)

    @staticmethod
    def validate_multi_stage(d, fname=None):
        Dvcfile._validate(COMPILED_MULTI_STAGE_SCHEMA, d, fname)

    @staticmethod
    def _validate(schema, d, fname=None):
        try:
            schema(d)
        except MultipleInvalid as exc:
            raise StageFileFormatError(fname, exc)

    def is_multi_stage(self, d=None):
        if d is None:
            d = self._load()[0]
        check_multi_stage = d.get("stages")
        if check_multi_stage:
            self.validate_multi_stage(d, self.path)
            return True

        self.validate_single_stage(d, self.path)
        return False

    def remove_with_prompt(self, force=False):
        if not self.exists():
            return

        msg = (
            "'{}' already exists. Do you wish to run the command and "
            "overwrite it?".format(relpath(self.path))
        )
        if not (force or prompt.confirm(msg)):
            raise StageFileAlreadyExistsError(self.path)

        os.unlink(self.path)

import os
import re
import logging

import dvc.prompt as prompt

from voluptuous import MultipleInvalid
from dvc import serialize
from dvc.exceptions import DvcException
from dvc.loader import SingleStageLoader, StageLoader
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


class FileMixin:
    def __init__(self, repo, path):
        self.repo = repo
        self.path, self.tag = self._get_path_tag(path)

    def __repr__(self):
        return "Dvcfile: {}".format(
            DVC_FILE, relpath(self.path, self.repo.root_dir)
        )

    def __str__(self):
        return "{}: {}".format(DVC_FILE, self.relpath)

    def relpath(self):
        return relpath(self.path)

    @classmethod
    def is_valid_filename(cls, path):
        return path.endswith(DVC_FILE_SUFFIX) or os.path.basename(path) in [
            DVC_FILE,
            "pipelines.yaml",
        ]

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
        self.validate(self.SCHEMA, d, self.path)
        return d, stage_text

    @staticmethod
    def validate(schema, d, fname=None):
        try:
            schema(d)
        except MultipleInvalid as exc:
            raise StageFileFormatError(fname, exc)

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


class SingleStageFile(FileMixin):
    from dvc.schema import COMPILED_SINGLE_STAGE_SCHEMA as SCHEMA

    @property
    def stage(self):
        data, raw = self._load()
        return SingleStageLoader.load_stage(self, data, raw)

    @property
    def stages(self):
        data, raw = self._load()
        return SingleStageLoader(self, data, raw)

    def dump(self, stage, **kwargs):
        """Dumps given stage appropriately in the dvcfile."""
        from dvc.stage import PipelineStage

        assert not isinstance(stage, PipelineStage)
        self.check_dvc_filename(self.path)
        logger.debug(
            "Saving information to '{file}'.".format(file=relpath(self.path))
        )
        dump_stage_file(self.path, serialize.to_single_stage_file(stage))
        self.repo.scm.track_file(relpath(self.path))


class PipelineFile(FileMixin):
    from dvc.schema import COMPILED_MULTI_STAGE_SCHEMA as SCHEMA

    @property
    def _lockfile(self):
        return os.path.splitext(self.path)[0] + ".lock"

    def dump(self, stage, update_pipeline=False):
        """Dumps given stage appropriately in the dvcfile."""
        from dvc.stage import PipelineStage

        assert isinstance(stage, PipelineStage)
        self._dump_lockfile(stage)
        if update_pipeline and not stage.is_data_source:
            self._dump_pipeline_file(stage)

    def _dump_lockfile(self, stage):
        from . import lockfile

        lockfile.dump(self.repo, self._lockfile, serialize.to_lockfile(stage))
        self.repo.scm.track_file(relpath(self._lockfile))

    def _dump_pipeline_file(self, stage):
        data = {}
        if self.exists():
            with open(self.path, "r") as fd:
                data = parse_stage_for_update(fd.read(), self.path)
        else:
            open(self.path, "w+").close()

        data["stages"] = data.get("stages", {})
        data["stages"].update(serialize.to_dvcfile(stage))

        dump_stage_file(self.path, self.SCHEMA(data))
        self.repo.scm.track_file(relpath(self.path))

    @property
    def stage(self):
        raise MultiStageFileLoadError(self.path)

    @property
    def stages(self):
        from . import lockfile

        data, raw = self._load()
        lockfile_data = lockfile.load(self.repo, self._lockfile)
        return StageLoader(self, data.get("stages", {}), lockfile_data)


class Dvcfile(FileMixin):
    def __new__(cls, repo, path):
        assert path
        _, ext = os.path.splitext(path)
        assert not ext or ext in [".yml", ".yaml", ".dvc"]

        if not ext or ext == ".dvc":
            return SingleStageFile(repo, path)
        return PipelineFile(repo, path)

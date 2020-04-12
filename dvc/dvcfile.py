import os
import re
import logging

import dvc.prompt as prompt

from voluptuous import MultipleInvalid

from dvc import dependency, output
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

    def dump(self, stage):
        """Dumps given stage appropriately in the dvcfile."""
        self.dump_single_stage(stage)

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

    def load(self):
        """Loads single stage."""
        from dvc.stage import Stage

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

        Dvcfile.validate(d, fname=relpath(self.path))
        path = os.path.abspath(self.path)

        stage = Stage(
            repo=self.repo,
            path=path,
            wdir=os.path.abspath(
                os.path.join(
                    os.path.dirname(path), d.get(Stage.PARAM_WDIR, ".")
                )
            ),
            cmd=d.get(Stage.PARAM_CMD),
            md5=d.get(Stage.PARAM_MD5),
            locked=d.get(Stage.PARAM_LOCKED, False),
            tag=self.tag,
            always_changed=d.get(Stage.PARAM_ALWAYS_CHANGED, False),
            # We store stage text to apply updates to the same structure
            stage_text=stage_text,
        )

        stage.deps = dependency.loadd_from(
            stage, d.get(Stage.PARAM_DEPS) or []
        )
        stage.outs = output.loadd_from(stage, d.get(Stage.PARAM_OUTS) or [])

        return stage

    @staticmethod
    def validate(d, fname=None):
        from dvc.stage.schema import SINGLE_STAGE_SCHEMA

        try:
            SINGLE_STAGE_SCHEMA(d)
        except MultipleInvalid as exc:
            raise StageFileFormatError(fname, exc)

    def overwrite_with_prompt(self, force=False):
        if not self.exists():
            return

        msg = (
            "'{}' already exists. Do you wish to run the command and "
            "overwrite it?".format(self.path)
        )
        if not (force or prompt.confirm(msg)):
            raise StageFileAlreadyExistsError(self.path)

        os.unlink(self.path)

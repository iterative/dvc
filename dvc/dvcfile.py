import collections
import json
import logging
import os
from typing import TYPE_CHECKING

from funcy import cached_property

from dvc import dependency, output
from dvc.utils import relpath, file_md5

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.stage import Stage

logger = logging.getLogger(__name__)


class Dvcfile:
    def __init__(self, repo: "Repo", path: str) -> None:
        self.path = path
        self.repo = repo
        self.is_multi_stages = False

    @cached_property
    def stages(self):
        from dvc.stage import Stage, PipelineStage
        from dvc.utils.stage import parse_stage

        fname, tag = Stage._get_path_tag(self.path)
        # it raises the proper exceptions by priority:
        # 1. when the file doesn't exists
        # 2. filename is not a DVC-file
        # 3. path doesn't represent a regular file
        Stage._check_file_exists(self.repo, fname)
        Stage._check_dvc_filename(fname)
        Stage._check_isfile(self.repo, fname)

        with self.repo.tree.open(fname) as fd:
            stage_text = fd.read()

        d = parse_stage(stage_text, fname)

        Stage.validate(d, fname=relpath(fname))
        path = os.path.abspath(fname)

        if not d.get("stages"):
            stages_obj = {fname: d}
            stage_cls = Stage
        else:
            # load lockfile and coerce
            lock_file = os.path.splitext(fname)[0] + ".lock"
            locks = {}
            if os.path.exists(lock_file):
                with open(lock_file) as fd:
                    locks = json.load(fd)

            self._coerce_stages_lock_deps(d, locks)
            self._coerce_stages_lock_outs(d, locks)
            self._coerce_stages_lock_stages(d, locks)

            stages_obj = d.get("stages", [])
            self.is_multi_stages = True
            stage_cls = PipelineStage

        stages = []
        for name, stage_obj in stages_obj.items():
            stage = stage_cls(
                repo=self.repo,
                path=path,
                wdir=os.path.abspath(
                    os.path.join(
                        os.path.dirname(path), d.get(Stage.PARAM_WDIR, ".")
                    )
                ),
                cmd=stage_obj.get(Stage.PARAM_CMD),
                md5=stage_obj.get(Stage.PARAM_MD5),
                locked=stage_obj.get(Stage.PARAM_LOCKED, False),
                tag=tag,
                always_changed=stage_obj.get(
                    Stage.PARAM_ALWAYS_CHANGED, False
                ),
                # We store stage text to apply updates to the same structure
                stage_text=stage_text if not d.get("stages") else None,
            )
            if stage_cls == PipelineStage:
                stage.name = name
                stage.dvcfile = self

            stage.deps = dependency.loadd_from(
                stage, stage_obj.get(Stage.PARAM_DEPS) or []
            )
            stage.outs = output.loadd_from(
                stage, stage_obj.get(Stage.PARAM_OUTS) or []
            )
            stages.append(stage)

        return stages

    def _coerce_stages_lock_outs(self, stages, locks):
        for stage_id, stage in stages["stages"].items():
            stage["outs"] = [
                {"path": item, **locks.get("outs", {}).get(item, {})}
                for item in stage.get("outs", [])
            ]

    def _coerce_stages_lock_deps(self, stages, locks):
        for stage_id, stage in stages["stages"].items():
            stage["deps"] = [
                {
                    "path": item,
                    **locks.get("deps", {}).get(stage_id, {}).get(item, {}),
                }
                for item in stage.get("deps", [])
            ]

    def _coerce_stages_lock_stages(self, stages, locks):
        for stage_id, stage in stages["stages"].items():
            stage["md5"] = locks.get("stages", {}).get(stage_id, {}).get("md5")

    def dump_multistages(self, stage, path="Dvcfile"):
        from dvc.utils.stage import parse_stage_for_update, dump_stage_file

        if not os.path.exists(path):
            open(path, "w+").close()

        with open(path, "r") as fd:
            data = parse_stage_for_update(fd.read(), path)

        # handle this in Stage::dumpd()
        data["stages"] = data.get("stages", {})
        data["stages"][stage.name] = {
            "cmd": stage.cmd,
            "deps": [dep.def_path for dep in stage.deps],
            "outs": [out.def_path for out in stage.outs],
        }

        dump_stage_file(path, data)
        self.repo.scm.track_file(path)

    def _dump_lockfile(self, stage):
        """
        {
            "md5": 0,
            "deps": {
                "1_generator": {
                    "1.txt": {
                        "md5": 1
                    },
                    "2.txt": {
                       "md5": 2
                    },
                    "3.txt": {
                      "md5": 3
                    }
                }
            },
            "outs": {
                "1.txt": {
                  "md5": 4
                },
                "2.txt": {
                    "md5": 5
                }
            },
            "stages": {
                "1_generator": {
                    "md5": 6
            }
        }
        """
        lockfile = os.path.splitext(stage.path)[0] + ".lock"

        if not os.path.exists(lockfile):
            open(lockfile, "w+").close()

        with open(lockfile, "r") as fd:
            try:
                lock = json.load(fd, object_pairs_hook=collections.OrderedDict)
            except json.JSONDecodeError:
                lock = collections.OrderedDict()

        print(lock)
        lock["md5"] = file_md5(stage.path)[0]
        lock["deps"] = lock.get("deps", {})
        lock["outs"] = lock.get("outs", {})
        lock["stages"] = lock.get("stages", {})

        lock["outs"].update(
            {
                out.def_path: {out.remote.PARAM_CHECKSUM: out.checksum}
                for out in stage.outs
                if out.checksum
            }
        )
        lock["deps"][stage.name] = {
            dep.def_path: {dep.remote.PARAM_CHECKSUM: dep.checksum}
            for dep in stage.deps
            if dep.checksum
        }
        lock["stages"][stage.name] = {"md5": stage.md5 or stage._compute_md5()}

        with open(lockfile, "w") as fd:
            json.dump(lock, fd)

        self.repo.scm.track_file(os.path.relpath(lockfile))

    def _dump_checkoutstage(self, stage):
        from dvc.stage import Stage

        for out in stage.outs:
            if not out.use_cache:
                continue

            s = Stage(
                stage.repo,
                # TODO: remove this after dependency graph collection is improved
                out.def_path + ".pipeline" + Stage.STAGE_FILE_SUFFIX,
            )
            s.outs = [out]
            s.md5 = s._compute_md5()
            s.dump()
            self.repo.scm.track_file(s.path)

    def dump(self, stage):
        from dvc.utils.stage import parse_stage_for_update
        from dvc.stage import Stage

        fname = stage.path
        Stage._check_dvc_filename(fname)

        logger.debug(
            "Saving information to '{file}'.".format(file=relpath(fname))
        )

        if not os.path.exists(fname):
            open(stage.path, "w+").close()

        with self.repo.tree.open(fname) as fd:
            text = fd.read()
        saved_state = parse_stage_for_update(text, fname)

        if saved_state.get("stages") or not (
            saved_state or stage.is_data_source
        ):
            self.is_multi_stages = True
            self._dump_lockfile(stage)
            self._dump_checkoutstage(stage)

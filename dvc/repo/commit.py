from typing import TYPE_CHECKING

from dvc import prompt

from . import locked
from .scm_context import scm_context

if TYPE_CHECKING:
    from . import Repo
    from .index import IndexView


def _prepare_message(stage, changes):
    changed_deps, changed_outs, changed_stage = changes
    if changed_deps and changed_outs:
        msg = "dependencies {deps} and outputs {outs} of {stage} changed."
    elif changed_deps:
        msg = "dependencies {deps} of {stage} changed."
    elif changed_outs:
        msg = "outputs {outs} of {stage} changed."
    else:
        msg = "{stage_changed}"

    msg += " Are you sure you want to commit it?"

    kw = {
        "stage": stage,
        "deps": changed_deps,
        "outs": changed_outs,
        "stage_changed": changed_stage,
    }
    return msg.format_map(kw)


def prompt_to_commit(stage, changes, force=False):
    from dvc.stage.exceptions import StageCommitError

    if not (force or prompt.confirm(_prepare_message(stage, changes))):
        raise StageCommitError(
            f"unable to commit changed {stage}. Use `-f|--force` to force."
        )


@locked
def commit(
    self,
    target=None,
    with_deps=False,
    recursive=False,
    force=False,
    allow_missing=False,
    data_only=False,
    relink=True,
):
    stages_info = [
        info
        for info in self.stage.collect_granular(
            target, with_deps=with_deps, recursive=recursive
        )
        if not data_only or info.stage.is_data_source
    ]
    for stage_info in stages_info:
        stage = stage_info.stage
        if force:
            stage.save(allow_missing=allow_missing)
        else:
            changes = stage.changed_entries()
            if any(changes):
                prompt_to_commit(stage, changes, force=force)
                stage.save(allow_missing=allow_missing)
        stage.commit(
            filter_info=stage_info.filter_info,
            allow_missing=allow_missing,
            relink=relink,
        )
        stage.dump(update_pipeline=False)
    return [s.stage for s in stages_info]


@locked
@scm_context
def commit_2_to_3(repo: "Repo", dry: bool = False):
    """Force-commit all legacy outputs to use DVC 3.0 hashes."""
    from dvc.dvcfile import ProjectFile
    from dvc.ui import ui

    view = repo.index.targets_view(
        targets=None,
        outs_filter=lambda o: o.hash_name == "md5-dos2unix",
        recursive=True,
    )
    migrated = _migrateable_dvcfiles(view)
    if not migrated:
        ui.write("No DVC files in the repo to migrate to the 3.0 format.")
        return
    if dry:
        ui.write("Entries in following DVC files will be migrated to the 3.0 format:")
        ui.write("\n".join(sorted(f"\t{file}" for file in migrated)))
        return
    for stage, filter_info in view._stage_infos:
        outs_filter = view._outs_filter
        outs = {
            out
            for out in stage.filter_outs(filter_info)
            if outs_filter is not None and outs_filter(out)
        }
        modified = False
        if outs:
            for out in outs:
                out.update_legacy_hash_name(force=True)
            modified = True
        deps = {dep for dep in stage.deps if not stage.is_import and dep.is_in_repo}
        if deps:
            for dep in deps:
                dep.update_legacy_hash_name(force=True)
            modified = True
        if modified:
            stage.save(allow_missing=True)
            stage.commit(allow_missing=True, relink=True)
            if not isinstance(stage.dvcfile, ProjectFile):
                ui.write(f"Updating DVC file '{stage.dvcfile.relpath}'")
            stage.dump(update_pipeline=False)


def _migrateable_dvcfiles(view: "IndexView") -> set[str]:
    from dvc.dvcfile import ProjectFile

    migrated = set()
    for stage, filter_info in view._stage_infos:
        outs_filter = view._outs_filter
        dvcfile = stage.dvcfile.relpath
        assert outs_filter
        if any(outs_filter(out) for out in stage.filter_outs(filter_info)) or (
            not stage.is_import
            and any(
                dep.is_in_repo and dep.hash_name == "md5-dos2unix" for dep in stage.deps
            )
        ):
            if isinstance(stage.dvcfile, ProjectFile):
                lockfile = stage.dvcfile._lockfile.relpath
                migrated.add(f"{dvcfile} ({lockfile})")
            else:
                migrated.add(dvcfile)
    return migrated

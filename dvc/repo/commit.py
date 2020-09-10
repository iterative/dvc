from dvc import prompt
from dvc.dvcfile import Dvcfile
from dvc.stage.exceptions import StageCommitError

from . import locked


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
    if not (force or prompt.confirm(_prepare_message(stage, changes))):
        raise StageCommitError(
            "unable to commit changed {}. Use `-f|--force` to "
            "force.".format(stage)
        )


@locked
def commit(self, target, with_deps=False, recursive=False, force=False):
    stages = self.collect(target, with_deps=with_deps, recursive=recursive)
    for stage in stages:
        changes = stage.changed_entries()
        if any(changes):
            prompt_to_commit(stage, changes, force=force)
            stage.save()
        stage.commit()

        Dvcfile(self, stage.path).dump(stage, update_pipeline=False)
    return stages

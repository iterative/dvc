from dvc import prompt
from dvc.dvcfile import Dvcfile
from dvc.stage.exceptions import StageCommitError

from . import locked


def prompt_to_commit(stage, changed_deps, changed_outs, force=False):
    kw = {"stage": stage}
    if changed_deps and changed_outs:
        kw.update({"deps": changed_deps, "outs": changed_outs})
        msg = "dependencies {deps} and outputs {outs} of {stage} changed. "
    elif changed_deps:
        kw["deps"] = changed_deps
        msg = "dependencies {deps} of {stage} changed. "
    elif changed_outs:
        kw["outs"] = changed_outs
        msg = "outputs {outs} of {stage} changed. "
    else:
        msg = "md5 of {stage} changed. "
    msg += "Are you sure you want to commit it?"

    if not (force or prompt.confirm(msg.format_map(kw))):
        raise StageCommitError(
            "unable to commit changed {}. Use `-f|--force` to "
            "force.".format(stage)
        )


@locked
def commit(self, target, with_deps=False, recursive=False, force=False):
    stages = self.collect(target, with_deps=with_deps, recursive=recursive)
    for stage in stages:
        changes = stage.changed_entries(force=force)
        if any(changes):
            changed_deps, changed_outs, _ = changes
            prompt_to_commit(stage, changed_deps, changed_outs, force=force)
            stage.save()
        stage.commit()

        Dvcfile(self, stage.path).dump(stage)

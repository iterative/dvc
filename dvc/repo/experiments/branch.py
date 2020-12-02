import logging

from dvc.exceptions import InvalidArgumentError
from dvc.repo import locked
from dvc.repo.scm_context import scm_context

logger = logging.getLogger(__name__)


@locked
@scm_context
def branch(repo, exp_rev, branch_name, *args, **kwargs):
    from dvc.repo.experiments.base import ExpRefInfo, InvalidExpRefError
    from dvc.scm.base import RevError
    from dvc.scm.git import Git

    ref_info = None
    if exp_rev.startswith("refs/"):
        try:
            ref_info = ExpRefInfo.from_ref(exp_rev)
        except InvalidExpRefError:
            pass
    elif Git.is_sha(exp_rev):
        try:
            rev = repo.scm.resolve_rev(exp_rev)
            ref = repo.experiments.get_branch_containing(rev)
            if ref:
                ref_info = ExpRefInfo.from_ref(ref)
        except RevError:
            pass

    if not ref_info:
        infos = list(repo.experiments.iter_ref_infos_by_name(exp_rev))
        if len(infos) == 1:
            ref_info = infos[0]
        else:
            current_rev = repo.scm.get_rev()
            for info in infos:
                if info.baseline_sha == current_rev:
                    ref_info = info
                    break
            if not ref_info:
                msg = [
                    f"Ambiguous experiment name '{exp_rev}' can refer to "
                    "multiple experiments. To create a branch use a full "
                    "experiment ref:",
                    "",
                ]
                msg.extend([str(info) for info in infos])
                raise InvalidArgumentError("\n".join(msg))

    if not ref_info:
        raise InvalidArgumentError(
            f"'{exp_rev}' does not appear to be a valid experiment."
        )

    branch_ref = f"refs/heads/{branch_name}"
    if repo.scm.get_ref(branch_ref):
        raise InvalidArgumentError(
            f"Git branch '{branch_name}' already exists."
        )

    target = repo.scm.get_ref(str(ref_info))
    repo.scm.set_ref(
        branch_ref,
        target,
        message=f"dvc: Created from experiment '{ref_info.name}'",
    )
    fmt = (
        "Git branch '%s' has been created from experiment '%s'.\n"
        "To switch to the new branch run:\n\n"
        "\tgit checkout %s"
    )
    logger.info(fmt, branch_name, ref_info.name, branch_name)

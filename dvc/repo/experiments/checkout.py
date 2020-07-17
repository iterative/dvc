import logging

from dvc.repo import locked
from dvc.repo.scm_context import scm_context

logger = logging.getLogger(__name__)


@locked
@scm_context
def checkout(repo, rev, *args, **kwargs):
    repo.experiments.checkout_exp(rev, *args, **kwargs)
    logger.info(
        "Changes for experiment '%s' have been applied to your current "
        "workspace.",
        rev,
    )

import logging

from funcy import once_per_args

from dvc.log import logger

logger = logger.getChild(__name__)


@once_per_args
def check_required_version(pkg: str, dist: str = "dvc", log_level=logging.WARNING):
    from importlib import metadata

    from packaging.requirements import InvalidRequirement, Requirement

    try:
        reqs = {
            r.name: r.specifier for r in map(Requirement, metadata.requires(dist) or [])
        }
        version = metadata.version(pkg)
    except (metadata.PackageNotFoundError, InvalidRequirement):
        return

    specifier = reqs.get(pkg)
    if specifier and version and version not in specifier:
        logger.log(
            log_level,
            "%s%s is required, but you have %r installed which is incompatible.",
            pkg,
            specifier,
            version,
        )

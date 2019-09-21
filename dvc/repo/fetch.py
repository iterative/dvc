from __future__ import unicode_literals

import logging

from dvc.config import NoRemoteError
from dvc.exceptions import DownloadError, OutputNotFoundError
from dvc.scm.base import CloneError


logger = logging.getLogger(__name__)


def _fetch(
    self,
    targets=None,
    jobs=None,
    remote=None,
    all_branches=False,
    show_checksums=False,
    with_deps=False,
    all_tags=False,
    recursive=False,
):
    """Download data items from a cloud and imported repositories

    Returns:
        int: number of succesfully downloaded files

    Raises:
        DownloadError: thrown when there are failed downloads, either
            during `cloud.pull` or trying to fetch imported files

        config.NoRemoteError: thrown when downloading only local files and no
            remote is configured
    """
    with self.state:
        used = self.used_cache(
            targets,
            all_branches=all_branches,
            all_tags=all_tags,
            with_deps=with_deps,
            force=True,
            remote=remote,
            jobs=jobs,
            recursive=recursive,
        )

        downloaded = 0
        failed = 0

        try:
            downloaded += self.cloud.pull(
                used["local"],
                jobs,
                remote=remote,
                show_checksums=show_checksums,
            )
        except NoRemoteError:
            if not used["repo"] and used["local"]:
                raise

        except DownloadError as exc:
            failed += exc.amount

        for dep in used["repo"]:
            try:
                out = dep.fetch()
                downloaded += out.get_files_number()
            except DownloadError as exc:
                failed += exc.amount
            except (CloneError, OutputNotFoundError):
                failed += 1
                logger.exception(
                    "failed to fetch data for '{}'".format(dep.stage.outs[0])
                )

        if failed:
            raise DownloadError(failed)

        return downloaded

from __future__ import unicode_literals

import collections
import logging


logger = logging.getLogger(__name__)


def _merge_cache_lists(clists):
    merged_cache = collections.defaultdict(list)

    for cache_list in clists:
        for scheme, cache in cache_list.items():
            for item in cache:
                if item not in merged_cache[scheme]:
                    merged_cache[scheme].append(item)

    return merged_cache


def _load_all_used_cache(
    repos,
    target=None,
    all_branches=False,
    active=True,
    with_deps=False,
    all_tags=False,
    remote=None,
    force=False,
    jobs=None,
):
    clists = []

    for repo in repos:
        with repo.state:
            repo_clist = repo.used_cache(
                target=None,
                all_branches=all_branches,
                active=False,
                with_deps=with_deps,
                all_tags=all_tags,
                remote=remote,
                force=force,
                jobs=jobs,
            )

            clists.append(repo_clist)

    return clists


def _do_gc(typ, func, clist):
    removed = func(clist)
    if not removed:
        logger.info("No unused {} cache to remove.".format(typ))


def gc(
    self,
    all_branches=False,
    cloud=False,
    remote=None,
    with_deps=False,
    all_tags=False,
    force=False,
    jobs=None,
    repos=None,
):
    from dvc.repo import Repo

    all_repos = [self]

    if repos:
        all_repos.extend(Repo(path) for path in repos)

    all_clists = _load_all_used_cache(
        all_repos,
        target=None,
        all_branches=all_branches,
        active=False,
        with_deps=with_deps,
        all_tags=all_tags,
        remote=remote,
        force=force,
        jobs=jobs,
    )

    if len(all_clists) > 1:
        clist = _merge_cache_lists(all_clists)
    else:
        clist = all_clists[0]

    with self.state:
        _do_gc("local", self.cache.local.gc, clist)

        if self.cache.s3:
            _do_gc("s3", self.cache.s3.gc, clist)

        if self.cache.gs:
            _do_gc("gs", self.cache.gs.gc, clist)

        if self.cache.ssh:
            _do_gc("ssh", self.cache.ssh.gc, clist)

        if self.cache.hdfs:
            _do_gc("hdfs", self.cache.hdfs.gc, clist)

        if self.cache.azure:
            _do_gc("azure", self.cache.azure.gc, clist)

        if cloud:
            _do_gc("remote", self.cloud._get_cloud(remote, "gc -c").gc, clist)

import logging
from collections import OrderedDict, defaultdict
from datetime import datetime
from typing import Optional

from funcy import first

from dvc.repo import locked
from dvc.repo.metrics.show import _collect_metrics, _read_metrics
from dvc.repo.params.show import _collect_configs, _read_params

logger = logging.getLogger(__name__)


def _collect_experiment(repo, rev, stash=False, sha_only=True):
    from git.exc import GitCommandError

    res = defaultdict(dict)
    for rev in repo.brancher(revs=[rev]):
        if rev == "workspace":
            res["timestamp"] = None
        else:
            commit = _resolve_commit(repo, rev)
            res["timestamp"] = datetime.fromtimestamp(commit.committed_date)

        configs = _collect_configs(repo, rev=rev)
        params = _read_params(repo, configs, rev)
        if params:
            res["params"] = params

        res["queued"] = stash
        if not stash:
            metrics = _collect_metrics(repo, None, False)
            vals = _read_metrics(repo, metrics, rev)
            res["metrics"] = vals

        if not sha_only and rev != "workspace":
            try:
                name = repo.scm.repo.git.describe(
                    rev, all=True, exact_match=True
                )
                name = name.rsplit("/")[-1]
                res["name"] = name
            except GitCommandError:
                pass

    return res


def _resolve_commit(repo, rev):
    from git.objects.tag import TagObject

    commit = repo.scm.repo.rev_parse(rev)
    if isinstance(commit, TagObject):
        commit = commit.object
    return commit


def _collect_checkpoint_experiment(repo, branch, baseline, **kwargs):
    res = OrderedDict()
    exp_rev = repo.scm.resolve_rev(branch)
    for rev in _branch_revs(repo, exp_rev, baseline):
        res[rev] = _collect_experiment(repo, rev, **kwargs)
        res[rev]["checkpoint_tip"] = exp_rev
    return res


def _branch_revs(repo, branch_tip, baseline: Optional[str] = None):
    """Iterate over revisions in a given branch (from newest to oldest).

    If baseline is set, iterator will stop when the specified revision is
    reached.
    """
    commit = _resolve_commit(repo, branch_tip)
    while commit is not None:
        yield commit.hexsha
        commit = first(commit.parents)
        if commit and commit.hexsha == baseline:
            return


@locked
def show(
    repo,
    all_branches=False,
    all_tags=False,
    revs=None,
    all_commits=False,
    sha_only=False,
):
    res = defaultdict(OrderedDict)

    if revs is None:
        revs = [repo.scm.get_rev()]

    revs = OrderedDict(
        (rev, None)
        for rev in repo.brancher(
            revs=revs,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            sha_only=True,
        )
    )

    for rev in revs:
        res[rev]["baseline"] = _collect_experiment(
            repo, rev, sha_only=sha_only
        )

    # collect reproduced experiments
    for exp_branch in repo.experiments.scm.list_branches():
        m = repo.experiments.BRANCH_RE.match(exp_branch)
        if m:
            rev = repo.scm.resolve_rev(m.group("baseline_rev"))
            if rev in revs:
                with repo.experiments.chdir():
                    if m.group("checkpoint"):
                        checkpoint_exps = _collect_checkpoint_experiment(
                            repo.experiments.exp_dvc, exp_branch, rev
                        )
                        res[rev].update(checkpoint_exps)
                    else:
                        exp_rev = repo.experiments.scm.resolve_rev(exp_branch)
                        experiment = _collect_experiment(
                            repo.experiments.exp_dvc, exp_branch
                        )
                        res[rev][exp_rev] = experiment

    # collect queued (not yet reproduced) experiments
    for stash_rev, entry in repo.experiments.stash_revs.items():
        if entry.baseline_rev in revs:
            with repo.experiments.chdir():
                experiment = _collect_experiment(
                    repo.experiments.exp_dvc, stash_rev, stash=True
                )
            res[entry.baseline_rev][stash_rev] = experiment

    return res

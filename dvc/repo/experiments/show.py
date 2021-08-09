import logging
from collections import OrderedDict, defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from dvc.exceptions import InvalidArgumentError
from dvc.repo import locked
from dvc.repo.experiments.base import ExpRefInfo
from dvc.repo.experiments.executor.base import ExecutorInfo
from dvc.repo.experiments.utils import fix_exp_head
from dvc.repo.metrics.show import _gather_metrics
from dvc.repo.params.show import _gather_params
from dvc.scm.base import SCMError
from dvc.utils import error_handler, onerror_collect

logger = logging.getLogger(__name__)


@error_handler
def _collect_experiment_commit(
    repo,
    exp_rev,
    stash=False,
    sha_only=True,
    param_deps=False,
    running=None,
    onerror: Optional[Callable] = None,
):
    res: Dict[str, Optional[Any]] = defaultdict(dict)
    for rev in repo.brancher(revs=[exp_rev]):
        if rev == "workspace":
            if exp_rev != "workspace":
                continue
            res["timestamp"] = None
        else:
            commit = repo.scm.resolve_commit(rev)
            res["timestamp"] = datetime.fromtimestamp(commit.commit_time)

        params = _gather_params(
            repo, rev=rev, targets=None, deps=param_deps, onerror=onerror
        )
        if params:
            res["params"] = params

        res["queued"] = stash
        if running is not None and exp_rev in running:
            res["running"] = True
            res["executor"] = running[exp_rev].get(ExecutorInfo.PARAM_LOCATION)
        else:
            res["running"] = False
            res["executor"] = None
        if not stash:
            vals = _gather_metrics(
                repo, targets=None, rev=rev, recursive=False, onerror=onerror
            )
            res["metrics"] = vals

        if not sha_only and rev != "workspace":
            for refspec in ["refs/tags", "refs/heads"]:
                name = repo.scm.describe(rev, base=refspec)
                if name:
                    break
            if not name:
                name = repo.experiments.get_exact_name(rev)
            if name:
                name = name.rsplit("/")[-1]
                res["name"] = name

    return res


def _collect_experiment_branch(
    res, repo, branch, baseline, onerror: Optional[Callable] = None, **kwargs
):
    exp_rev = repo.scm.resolve_rev(branch)
    prev = None
    revs = list(repo.scm.branch_revs(exp_rev, baseline))
    for rev in revs:
        collected_exp = _collect_experiment_commit(
            repo, rev, onerror=onerror, **kwargs
        )
        if len(revs) > 1:
            exp = {"checkpoint_tip": exp_rev}
            if prev:
                res[prev]["data"][  # type: ignore[unreachable]
                    "checkpoint_parent"
                ] = rev
            if rev in res:
                res[rev]["data"].update(exp)
                res.move_to_end(rev)
            else:
                exp.update(collected_exp["data"])
        else:
            exp = collected_exp["data"]
        if rev not in res:
            res[rev] = {"data": exp}
        prev = rev
    if len(revs) > 1:
        res[prev]["data"]["checkpoint_parent"] = baseline
    return res


@locked
def show(
    repo,
    all_branches=False,
    all_tags=False,
    revs=None,
    all_commits=False,
    sha_only=False,
    num=1,
    param_deps=False,
    onerror: Optional[Callable] = None,
):
    if onerror is None:
        onerror = onerror_collect

    res: Dict[str, Dict] = defaultdict(OrderedDict)

    if num < 1:
        raise InvalidArgumentError(f"Invalid number of commits '{num}'")

    if revs is None:
        revs = []
        for n in range(num):
            try:
                head = fix_exp_head(repo.scm, f"HEAD~{n}")
                revs.append(repo.scm.resolve_rev(head))
            except SCMError:
                break

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

    running = repo.experiments.get_running_exps()

    for rev in revs:
        res[rev]["baseline"] = _collect_experiment_commit(
            repo,
            rev,
            sha_only=sha_only,
            param_deps=param_deps,
            running=running,
            onerror=onerror,
        )

        if rev == "workspace":
            continue

        ref_info = ExpRefInfo(baseline_sha=rev)
        commits = [
            (ref, repo.scm.resolve_commit(ref))
            for ref in repo.scm.iter_refs(base=str(ref_info))
        ]
        for exp_ref, _ in sorted(
            commits, key=lambda x: x[1].commit_time, reverse=True
        ):
            ref_info = ExpRefInfo.from_ref(exp_ref)
            assert ref_info.baseline_sha == rev
            _collect_experiment_branch(
                res[rev],
                repo,
                exp_ref,
                rev,
                sha_only=sha_only,
                param_deps=param_deps,
                running=running,
                onerror=onerror,
            )
        # collect queued (not yet reproduced) experiments
        for stash_rev, entry in repo.experiments.stash_revs.items():
            if entry.baseline_rev in revs:
                if stash_rev not in running or not running[stash_rev].get(
                    "last"
                ):
                    experiment = _collect_experiment_commit(
                        repo,
                        stash_rev,
                        sha_only=sha_only,
                        stash=stash_rev not in running,
                        param_deps=param_deps,
                        running=running,
                        onerror=onerror,
                    )
                    res[entry.baseline_rev][stash_rev] = experiment
    return res

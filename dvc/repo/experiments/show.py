import logging
from collections import OrderedDict, defaultdict
from datetime import datetime
from enum import Enum
from itertools import chain
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

from dvc.repo.metrics.show import _gather_metrics
from dvc.repo.params.show import _gather_params
from dvc.scm import iter_revs
from dvc.utils import error_handler, onerror_collect, relpath

from .refs import ExpRefInfo

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


class ExpStatus(Enum):
    Success = 0
    Queued = 1
    Running = 2
    Failed = 3


@error_handler
def _collect_experiment_commit(
    repo: "Repo",
    exp_rev: str,
    status: ExpStatus = ExpStatus.Success,
    param_deps=False,
    running=None,
    onerror: Optional[Callable] = None,
):
    from dvc.dependency import ParamsDependency, RepoDependency

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

        res["deps"] = {
            relpath(dep.fs_path, repo.root_dir): {
                "hash": dep.hash_info.value,
                "size": dep.meta.size,
                "nfiles": dep.meta.nfiles,
            }
            for dep in repo.index.deps
            if not isinstance(dep, (ParamsDependency, RepoDependency))
        }
        res["outs"] = {
            relpath(out.fs_path, repo.root_dir): {
                "hash": out.hash_info.value,
                "size": out.meta.size,
                "nfiles": out.meta.nfiles,
                "use_cache": out.use_cache,
                "is_data_source": out.stage.is_data_source,
            }
            for out in repo.index.outs
            if not (out.is_metric or out.is_plot)
        }

        res["status"] = status.name
        if status == ExpStatus.Running:
            res["executor"] = running.get(exp_rev, {}).get("location", None)
        else:
            res["executor"] = None

        if status == ExpStatus.Failed:
            res["error"] = {
                "msg": "Experiment run failed.",
                "type": "",
            }

        if status not in {ExpStatus.Queued, ExpStatus.Failed}:
            vals = _gather_metrics(
                repo, targets=None, rev=rev, recursive=False, onerror=onerror
            )
            res["metrics"] = vals

    return res


def _collect_experiment_branch(
    res,
    repo,
    branch,
    baseline,
    onerror: Optional[Callable] = None,
    running=None,
    **kwargs,
):
    from dvc.scm import resolve_rev

    exp_rev = resolve_rev(repo.scm, branch)
    prev = None
    revs = list(repo.scm.branch_revs(exp_rev, baseline))
    for rev in revs:
        status = ExpStatus.Running if rev in running else ExpStatus.Success
        collected_exp = _collect_experiment_commit(
            repo,
            rev,
            onerror=onerror,
            status=status,
            running=running,
            **kwargs,
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


def get_names(repo: "Repo", result: Dict[str, Dict[str, Any]]):

    rev_set = set()
    baseline_set = set()
    for baseline in result:
        for rev in result[baseline]:
            if rev == "baseline":
                rev = baseline
                baseline_set.add(rev)
            if rev != "workspace":
                rev_set.add(rev)

    names: Dict[str, Optional[str]] = {}
    for base in ("refs/tags/", "refs/heads/"):
        if rev_set:
            names.update(
                (rev, ref[len(base) :])
                for rev, ref in repo.scm.describe(
                    baseline_set, base=base
                ).items()
                if ref is not None
            )
            rev_set.difference_update(names.keys())

    exact_name = repo.experiments.get_exact_name(rev_set)

    for baseline, baseline_results in result.items():
        for rev, rev_result in baseline_results.items():
            name: Optional[str] = None
            if rev == "baseline":
                rev = baseline
                if rev == "workspace":
                    continue
                name = names.get(rev, None)
            name = name or exact_name[rev]
            if name:
                rev_result["data"]["name"] = name


# flake8: noqa: C901
def _collect_active_experiment(
    repo: "Repo",
    found_revs: Dict[str, List[str]],
    running: Dict[str, Any],
    **kwargs,
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict] = defaultdict(OrderedDict)
    for entry in chain(
        repo.experiments.tempdir_queue.iter_active(),
        repo.experiments.celery_queue.iter_active(),
    ):
        stash_rev = entry.stash_rev
        if entry.baseline_rev in found_revs and (
            stash_rev not in running or not running[stash_rev].get("last")
        ):
            result[entry.baseline_rev][stash_rev] = _collect_experiment_commit(
                repo,
                stash_rev,
                status=ExpStatus.Running,
                running=running,
                **kwargs,
            )
    return result


def _collect_queued_experiment(
    repo: "Repo",
    found_revs: Dict[str, List[str]],
    running: Dict[str, Any],
    **kwargs,
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict] = defaultdict(OrderedDict)
    for entry in repo.experiments.celery_queue.iter_queued():
        stash_rev = entry.stash_rev
        if entry.baseline_rev in found_revs:
            result[entry.baseline_rev][stash_rev] = _collect_experiment_commit(
                repo,
                stash_rev,
                status=ExpStatus.Queued,
                running=running,
                **kwargs,
            )
    return result


def _collect_failed_experiment(
    repo: "Repo",
    found_revs: Dict[str, List[str]],
    running: Dict[str, Any],
    **kwargs,
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict] = defaultdict(OrderedDict)
    for queue_done_result in repo.experiments.celery_queue.iter_failed():
        entry = queue_done_result.entry
        stash_rev = entry.stash_rev
        if entry.baseline_rev in found_revs:
            experiment = _collect_experiment_commit(
                repo,
                stash_rev,
                status=ExpStatus.Failed,
                running=running,
                **kwargs,
            )
            result[entry.baseline_rev][stash_rev] = experiment
    return result


def update_new(
    to_dict: Dict[str, Dict[str, Any]], from_dict: Dict[str, Dict[str, Any]]
):
    for baseline, experiments in from_dict.items():
        for rev, experiment in experiments.items():
            to_dict[baseline][rev] = to_dict[baseline].get(rev, experiment)


def show(
    repo: "Repo",
    all_branches=False,
    all_tags=False,
    revs: Union[List[str], str, None] = None,
    all_commits=False,
    hide_queued=False,
    hide_failed=False,
    sha_only=False,
    num=1,
    param_deps=False,
    onerror: Optional[Callable] = None,
    fetch_running: bool = True,
):

    if repo.scm.no_commits:
        return {}

    if onerror is None:
        onerror = onerror_collect

    res: Dict[str, Dict] = defaultdict(OrderedDict)

    if not any([revs, all_branches, all_tags, all_commits]):
        revs = ["HEAD"]
    if isinstance(revs, str):
        revs = [revs]

    found_revs: Dict[str, List[str]] = {"workspace": []}
    found_revs.update(
        iter_revs(repo.scm, revs, num, all_branches, all_tags, all_commits)
    )

    running: Dict[str, Dict] = repo.experiments.get_running_exps(
        fetch_refs=fetch_running
    )

    queued_experiment = (
        _collect_queued_experiment(
            repo,
            found_revs,
            running,
            param_deps=param_deps,
            onerror=onerror,
        )
        if not hide_queued
        else {}
    )

    active_experiment = _collect_active_experiment(
        repo,
        found_revs,
        running,
        param_deps=param_deps,
        onerror=onerror,
    )

    failed_experiments = (
        _collect_failed_experiment(
            repo,
            found_revs,
            running,
            sha_only=sha_only,
            param_deps=param_deps,
            onerror=onerror,
        )
        if not hide_failed
        else {}
    )

    for rev in found_revs:
        status = ExpStatus.Running if rev in running else ExpStatus.Success
        res[rev]["baseline"] = _collect_experiment_commit(
            repo,
            rev,
            status=status,
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
                param_deps=param_deps,
                running=running,
                onerror=onerror,
            )

    if not hide_failed:
        update_new(res, failed_experiments)

    update_new(res, active_experiment)

    if not hide_queued:
        update_new(res, queued_experiment)

    if not sha_only:
        get_names(repo, res)

    return res

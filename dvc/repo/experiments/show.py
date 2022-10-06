import logging
from collections import OrderedDict, defaultdict
from datetime import datetime
from enum import Enum
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from scmrepo.exceptions import SCMError as InnerScmError

from dvc.repo.metrics.show import _gather_metrics
from dvc.repo.params.show import _gather_params
from dvc.scm import SCMError, iter_revs, resolve_rev
from dvc.utils import error_handler, onerror_collect, relpath

from .refs import ExpRefInfo

if TYPE_CHECKING:
    from scmrepo.git.objects import GitCommit

    from dvc.repo import Repo

logger = logging.getLogger(__name__)


class ExpStatus(Enum):
    Success = 0
    Queued = 1
    Running = 2
    Failed = 3


def _is_scm_error(collected_exp: Dict[str, Any]) -> bool:
    if "error" in collected_exp and (
        isinstance(collected_exp["error"], SCMError)
        or isinstance(collected_exp["error"], InnerScmError)
    ):
        return True
    return False


def _show_onerror_collect(result: Dict, exception: Exception, *args, **kwargs):
    onerror_collect(result, exception, *args, **kwargs)
    result["data"] = {}


@error_handler
def collect_experiment_commit(
    repo: "Repo",
    exp_rev: str,
    status: ExpStatus = ExpStatus.Success,
    param_deps=False,
    running: Optional[Dict[str, Any]] = None,
    onerror: Optional[Callable] = None,
) -> Dict[str, Any]:
    from dvc.dependency import ParamsDependency, RepoDependency

    result: Dict[str, Any] = defaultdict(dict)
    running = running or {}
    for rev in repo.brancher(revs=[exp_rev]):
        if rev == "workspace":
            if exp_rev != "workspace":
                continue
            result["timestamp"] = None
        else:
            commit = repo.scm.resolve_commit(rev)
            result["timestamp"] = datetime.fromtimestamp(commit.commit_time)

        params = _gather_params(
            repo, rev=rev, targets=None, deps=param_deps, onerror=onerror
        )
        if params:
            result["params"] = params

        result["deps"] = {
            relpath(dep.fs_path, repo.root_dir): {
                "hash": dep.hash_info.value,
                "size": dep.meta.size,
                "nfiles": dep.meta.nfiles,
            }
            for dep in repo.index.deps
            if not isinstance(dep, (ParamsDependency, RepoDependency))
        }
        result["outs"] = {
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

        result["status"] = status.name
        if status == ExpStatus.Running:
            result["executor"] = running.get(exp_rev, {}).get("location", None)
        else:
            result["executor"] = None

        if status == ExpStatus.Failed:
            result["error"] = {
                "msg": "Experiment run failed.",
                "type": "",
            }

        if status not in {ExpStatus.Queued, ExpStatus.Failed}:
            vals = _gather_metrics(
                repo, targets=None, rev=rev, recursive=False, onerror=onerror
            )
            result["metrics"] = vals

    return result


def _collect_complete_experiment(
    repo: "Repo",
    baseline: str,
    exp_rev: str,
    running: Dict[str, Any],
    revs: List[str],
    **kwargs,
) -> Dict[str, Dict[str, Any]]:
    results: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

    checkpoint: bool = True if len(revs) > 1 else False
    prev = ""
    for rev in revs:
        status = ExpStatus.Running if rev in running else ExpStatus.Success
        collected_exp = collect_experiment_commit(
            repo,
            rev,
            status=status,
            running=running,
            **kwargs,
        )
        if _is_scm_error(collected_exp):
            return {}
        if checkpoint:
            exp = {"checkpoint_tip": exp_rev}
            if prev:
                results[prev]["data"][  # type: ignore[unreachable]
                    "checkpoint_parent"
                ] = rev
            if rev in results:
                results[rev]["data"].update(exp)
                results.move_to_end(rev)
            else:
                exp.update(collected_exp["data"])
        else:
            exp = collected_exp["data"]
        if rev not in results:
            results[rev] = {"data": exp}
        prev = rev
    if checkpoint and prev:
        results[prev]["data"]["checkpoint_parent"] = baseline
    return results


def _collect_branch(
    repo: "Repo",
    baseline: str,
    running: Dict[str, Any],
    **kwargs,
) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = OrderedDict()
    status = ExpStatus.Running if baseline in running else ExpStatus.Success
    results["baseline"] = collect_experiment_commit(
        repo,
        baseline,
        status=status,
        running=running,
        **kwargs,
    )
    if baseline == "workspace" or _is_scm_error(results["baseline"]):
        return results

    ref_info = ExpRefInfo(baseline_sha=baseline)
    commits: List[Tuple[str, "GitCommit", str, List[str]]] = []

    for ref in repo.scm.iter_refs(base=str(ref_info)):
        try:
            commit = repo.scm.resolve_commit(ref)
            exp_rev = resolve_rev(repo.scm, ref)
            revs = list(repo.scm.branch_revs(exp_rev, baseline))
        except (SCMError, InnerScmError):
            continue
        commits.append((ref, commit, exp_rev, revs))

    for exp_ref, _, exp_rev, revs in sorted(
        commits, key=lambda x: x[1].commit_time, reverse=True
    ):
        ref_info = ExpRefInfo.from_ref(exp_ref)
        assert ref_info.baseline_sha == baseline
        collected_exp = _collect_complete_experiment(
            repo,
            baseline=baseline,
            exp_rev=exp_rev,
            running=running,
            revs=revs,
            **kwargs,
        )
        if _is_scm_error(collected_exp):
            continue
        results.update(collected_exp)
    return results


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
            name = names.get(rev, None) or exact_name[rev]
            if name:
                rev_result["data"]["name"] = name


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
            collected_exp = collect_experiment_commit(
                repo,
                stash_rev,
                status=ExpStatus.Running,
                running=running,
                **kwargs,
            )
            if _is_scm_error(collected_exp):
                continue
            result[entry.baseline_rev][stash_rev] = collected_exp
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
            collected_exp = collect_experiment_commit(
                repo,
                stash_rev,
                status=ExpStatus.Queued,
                running=running,
                **kwargs,
            )
            if _is_scm_error(collected_exp):
                continue
            result[entry.baseline_rev][stash_rev] = collected_exp
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
            collected_exp = collect_experiment_commit(
                repo,
                stash_rev,
                status=ExpStatus.Failed,
                running=running,
                **kwargs,
            )
            if _is_scm_error(collected_exp):
                continue
            result[entry.baseline_rev][stash_rev] = collected_exp
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
        onerror = _show_onerror_collect

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
            param_deps=param_deps,
            onerror=onerror,
        )
        if not hide_failed
        else {}
    )

    for baseline in found_revs:
        res[baseline] = _collect_branch(
            repo,
            baseline,
            running=running,
            param_deps=param_deps,
            onerror=onerror,
        )

    update_new(res, failed_experiments)

    update_new(res, active_experiment)

    update_new(res, queued_experiment)

    if not sha_only:
        get_names(repo, res)

    return res

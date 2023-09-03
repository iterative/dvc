from typing import TYPE_CHECKING, Dict, TypedDict, Union

from funcy import compact

from dvc.utils.diff import diff as _diff
from dvc.utils.diff import format_dict

if TYPE_CHECKING:
    from dvc.repo import Repo


class DiffResult(TypedDict, total=False):
    errors: Dict[str, Union[Exception, Dict[str, Exception]]]
    diff: Dict[str, Dict[str, Dict]]


def diff(repo: "Repo", *args, a_rev=None, b_rev=None, **kwargs) -> DiffResult:
    if repo.scm.no_commits:
        return {}

    with_unchanged = kwargs.pop("all", False)

    a_rev = a_rev or "HEAD"
    b_rev = b_rev or "workspace"

    metrics = repo.metrics.show(
        *args,
        **kwargs,
        revs=[a_rev, b_rev],
        hide_workspace=False,
        on_error="return",
    )

    old = metrics.get(a_rev, {})
    new = metrics.get(b_rev, {})

    old_data = old.get("data", {})
    new_data = new.get("data", {})

    result = DiffResult()

    errors = result.setdefault("errors", {})
    if old_error := old.get("error"):
        errors[a_rev] = old_error
    else:
        errors[a_rev] = {f: d["error"] for f, d in old_data.items() if "error" in d}

    if new_error := new.get("error"):
        errors[b_rev] = new_error
    else:
        errors[b_rev] = {f: d["error"] for f, d in new_data.items() if "error" in d}

    diff_result = _diff(
        format_dict(old_data), format_dict(new_data), with_unchanged=with_unchanged
    )
    result = DiffResult(errors=errors, diff=diff_result)
    result["errors"] = compact(result.get("errors", {}))  # type: ignore[assignment]
    return compact(result)  # type: ignore[no-any-return]

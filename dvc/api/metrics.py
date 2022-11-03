from collections import Counter
from typing import Dict, Optional

from dvc.exceptions import DvcException
from dvc.repo import Repo


def metrics_show(
    *targets: str,
    all_branches: Optional[bool] = False,
    all_tags: Optional[bool] = False,
    all_commits: Optional[bool] = False,
    recursive: Optional[bool] = False,
    repo: Optional[str] = None,
    rev: Optional[str] = None,
) -> Dict:
    """Get metrics tracked in `repo`.

    Without arguments, this function will retrieve all metrics from all tracked
    metric files, for the current working tree.

    See the options below to restrict the metrics retrieved.

    Args:
        *targets (str, optional): Names of the metric files to retrieve
        params from. For example, "classifier_eval.json, clustering_eval.json".
        If no `targets` are provided, all metric files tracked in `dvc.yaml`
        will be used.
        Note that targets don't necessarily have to be defined in `dvc.yaml`.
        all_branches (bool, optional): Whether to show for all repo branches
            or not.
            Defaults to `False`.
        all_tags (bool, optional): Whether to show for all repo tags or not.
            Defaults to `False`.
        all_commits (bool, optional): Whether to show for all commits or not.
            Defaults to `False`.
        recursive (bool, optional): Whether to recurse directories or not.
            Defaults to `False`.
        repo (str, optional): Location of the DVC repository.
            Defaults to the current project (found by walking up from the
            current working directory tree).
            It can be a URL or a file system path.
            Both HTTP and SSH protocols are supported for online Git repos
            (e.g. [user@]server:project.git).
        rev (str, optional): Name of the `Git revision`_ to retrieve metrics
            from.
            Defaults to `None`.
            An example of git revision can be a branch or tag name, a commit
            hash or a dvc experiment name.
            If `repo` is not a Git repo, this option is ignored.
            If `None`, the current working tree will be used.

    Returns:
        Dict: See Examples below.

    Raises:
        DvcException: If no params are found in `repo`.

    Examples:

        - No arguments.

        Working on https://github.com/iterative/example-get-started

        >>> import dvc.api
        >>> metrics = dvc.api.metrics_show()
        >>> print(json.dumps(metrics, indent=4))
        {
            "avg_prec": 0.9249974999612706,
            "roc_auc": 0.9460213440787918
        }

        ---

        - Using `rev`.

        Working on https://github.com/iterative/example-get-started

        >>> import json
        >>> import dvc.api
        >>> metrics = dvc.api.metrics_show(rev="tune-hyperparams")
        >>> print(json.dumps(metrics, indent=4))
        {
            "avg_prec": 0.9268792615819422,
            "roc_auc": 0.945093365854111
        }

        ---

        - Using `targets`.

        Working on https://github.com/iterative/example-get-started

        >>> import json
        >>> import dvc.api
        >>> metrics = dvc.api.metrics_show("evaluation.json")
        >>> print(json.dumps(metrics, indent=4))
        {
            "avg_prec": 0.9249974999612706,
            "roc_auc": 0.9460213440787918
        }

        ---

        - Git URL as `repo`.

        >>> import json
        >>> import dvc.api
        >>> metrics = dvc.api.metrics_show(
        ...     repo="https://github.com/iterative/demo-fashion-mnist")
        >>> print(json.dumps(metrics, indent=4))
        {
            "loss": 0.25284987688064575,
            "accuracy": 0.9071000218391418
        }


    .. _Git revision:
        https://git-scm.com/docs/revisions

    """

    def _onerror_raise(result: Dict, exception: Exception, *args, **kwargs):
        raise exception

    def _postprocess(metrics):
        processed = {}
        for rev, rev_data in metrics.items():
            if not rev_data:
                continue

            processed[rev] = {}

            counts = Counter()
            for file_data in rev_data["data"].values():
                for k in file_data["data"]:
                    counts[k] += 1

            for file_name, file_data in rev_data["data"].items():
                to_merge = {
                    (k if counts[k] == 1 else f"{file_name}:{k}"): v
                    for k, v in file_data["data"].items()
                }
                processed[rev] = {**processed[rev], **to_merge}

        if not processed:
            raise DvcException("No metrics found")

        return processed

    with Repo.open(repo) as _repo:
        metrics = _repo.metrics.show(
            targets=targets,
            all_branches=all_branches,
            all_tags=all_tags,
            recursive=recursive,
            revs=rev if rev is None else [rev],
            all_commits=all_commits,
            onerror=_onerror_raise,
        )

    return _postprocess(metrics)

from collections import Counter
from typing import Dict, Optional

from funcy import first

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
    """Get metrics tracked in `repo`"""

    def _onerror_raise(result: Dict, exception: Exception, *args, **kwargs):
        raise exception

    def _postprocess(metrics):
        processed = {}
        for rev, rev_data in metrics.items():
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

        if "workspace" in processed:
            del processed["workspace"]

        if not processed:
            raise DvcException("No metrics found")

        return processed[first(processed)]

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

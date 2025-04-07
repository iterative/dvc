import typing
from collections import Counter
from collections.abc import Iterable
from typing import Optional, Union

from funcy import first

from dvc.repo import Repo


def _postprocess(results):
    processed: dict[str, dict] = {}
    for rev, rev_data in results.items():
        if not rev_data:
            continue

        processed[rev] = {}

        counts: typing.Counter[str] = Counter()
        for file_data in rev_data["data"].values():
            for k in file_data["data"]:
                counts[k] += 1
        for file_name, file_data in rev_data["data"].items():
            to_merge = {
                (k if counts[k] == 1 else f"{file_name}:{k}"): v
                for k, v in file_data["data"].items()
            }
            processed[rev] = processed[rev] | to_merge

    processed.pop("workspace", None)

    return processed


def metrics_show(
    *targets: str,
    repo: Optional[str] = None,
    rev: Optional[str] = None,
    config: Optional[dict] = None,
) -> dict:
    """Get metrics tracked in `repo`.

    Without arguments, this function will retrieve all metrics from all tracked
    metric files, for the current working tree.

    See the options below to restrict the metrics retrieved.

    Args:
        *targets (str, optional): Names of the metric files to retrieve
        metrics from. For example, "classifier_eval.json,
        clustering_eval.json".
        If no `targets` are provided, all metric files tracked in `dvc.yaml`
        will be used.
        Note that targets don't necessarily have to be defined in `dvc.yaml`.
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
        config (dict, optional): config to be passed through to DVC project.
            Defaults to `None`.

    Returns:
        Dict: See Examples below.

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
    from dvc.repo.metrics.show import to_relpath

    with Repo.open(repo, config=config) as _repo:
        metrics = _repo.metrics.show(
            targets=targets,
            revs=rev if rev is None else [rev],
            on_error="raise",
        )
        metrics = {
            k: to_relpath(_repo.fs, _repo.root_dir, v) for k, v in metrics.items()
        }

    metrics = _postprocess(metrics)

    if not metrics:
        return {}

    return metrics[first(metrics)]


def params_show(
    *targets: str,
    repo: Optional[str] = None,
    stages: Optional[Union[str, Iterable[str]]] = None,
    rev: Optional[str] = None,
    deps: bool = False,
    config: Optional[dict] = None,
) -> dict:
    """Get parameters tracked in `repo`.

    Without arguments, this function will retrieve all params from all tracked
    parameter files, for the current working tree.

    See the options below to restrict the parameters retrieved.

    Args:
        *targets (str, optional): Names of the parameter files to retrieve
        params from. For example, "params.py, myparams.toml".
        If no `targets` are provided, all parameter files tracked in `dvc.yaml`
        will be used.
        Note that targets don't necessarily have to be defined in `dvc.yaml`.
        repo (str, optional): location of the DVC repository.
            Defaults to the current project (found by walking up from the
            current working directory tree).
            It can be a URL or a file system path.
            Both HTTP and SSH protocols are supported for online Git repos
            (e.g. [user@]server:project.git).
        stages (Union[str, Iterable[str]], optional): Name or names of the
            stages to retrieve parameters from.
            Defaults to `None`.
            If `None`, all parameters from all stages will be retrieved.
            If this method is called from a different location to the one where
            the `dvc.yaml` is found, the relative path to the `dvc.yaml` must
            be provided as a prefix with the syntax `{relpath}:{stage}`.
            For example: `subdir/dvc.yaml:stage-0` or `../dvc.yaml:stage-1`.
        rev (str, optional): Name of the `Git revision`_ to retrieve parameters
            from.
            Defaults to `None`.
            An example of git revision can be a branch or tag name, a commit
            hash or a dvc experiment name.
            If `repo` is not a Git repo, this option is ignored.
            If `None`, the current working tree will be used.
        deps (bool, optional): Whether to retrieve only parameters that are
            stage dependencies or not.
            Defaults to `False`.
        config (dict, optional): config to be passed through to DVC project.
            Defaults to `None`.

    Returns:
        Dict: See Examples below.

    Examples:

        - No arguments.

        Working on https://github.com/iterative/example-get-started

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show()
        >>> print(json.dumps(params, indent=4))
        {
            "prepare": {
                "split": 0.2,
                "seed": 20170428
            },
            "featurize": {
                "max_features": 200,
                "ngrams": 2
            },
            "train": {
                "seed": 20170428,
                "n_est": 50,
                "min_split": 0.01
            }
        }

        ---

        - Filtering with `stages`.

        Working on https://github.com/iterative/example-get-started

        `stages` can a single string:

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show(stages="prepare")
        >>> print(json.dumps(params, indent=4))
        {
            "prepare": {
                "split": 0.2,
                "seed": 20170428
            }
        }

        Or an iterable of strings:

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show(stages=["prepare", "train"])
        >>> print(json.dumps(params, indent=4))
        {
            "prepare": {
                "split": 0.2,
                "seed": 20170428
            },
            "train": {
                "seed": 20170428,
                "n_est": 50,
                "min_split": 0.01
            }
        }

        ---

        - Using `rev`.

        Working on https://github.com/iterative/example-get-started

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show(rev="tune-hyperparams")
        >>> print(json.dumps(params, indent=4))
        {
            "prepare": {
                "split": 0.2,
                "seed": 20170428
            },
            "featurize": {
                "max_features": 200,
                "ngrams": 2
            },
            "train": {
                "seed": 20170428,
                "n_est": 100,
                "min_split": 8
            }
        }

        ---

        - Using `targets`.

        Working on `multi-params-files` folder of
        https://github.com/iterative/pipeline-conifguration

        You can pass a single target:

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show("params.yaml")
        >>> print(json.dumps(params, indent=4))
        {
            "run_mode": "prod",
            "configs": {
                "dev": "configs/params_dev.yaml",
                "test": "configs/params_test.yaml",
                "prod": "configs/params_prod.yaml"
            },
            "evaluate": {
                "dataset": "micro",
                "size": 5000,
                "metrics": ["f1", "roc-auc"],
                "metrics_file": "reports/metrics.json",
                "plots_cm": "reports/plot_confusion_matrix.png"
            }
        }


        Or multiple targets:

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show(
        ...     "configs/params_dev.yaml", "configs/params_prod.yaml")
        >>> print(json.dumps(params, indent=4))
        {
            "configs/params_prod.yaml:run_mode": "prod",
            "configs/params_prod.yaml:config_file": "configs/params_prod.yaml",
            "configs/params_prod.yaml:data_load": {
                "dataset": "large",
                "sampling": {
                "enable": true,
                "size": 50000
                }
            },
            "configs/params_prod.yaml:train": {
                "epochs": 1000
            },
            "configs/params_dev.yaml:run_mode": "dev",
            "configs/params_dev.yaml:config_file": "configs/params_dev.yaml",
            "configs/params_dev.yaml:data_load": {
                "dataset": "development",
                "sampling": {
                "enable": true,
                "size": 1000
                }
            },
            "configs/params_dev.yaml:train": {
                "epochs": 10
            }
        }

        ---

        - Git URL as `repo`.

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show(
        ...     repo="https://github.com/iterative/demo-fashion-mnist")
        {
            "train": {
                "batch_size": 128,
                "hidden_units": 64,
                "dropout": 0.4,
                "num_epochs": 10,
                "lr": 0.001,
                "conv_activation": "relu"
            }
        }


    .. _Git revision:
        https://git-scm.com/docs/revisions

    """
    from dvc.repo.metrics.show import to_relpath

    if isinstance(stages, str):
        stages = [stages]

    with Repo.open(repo, config=config) as _repo:
        params = _repo.params.show(
            revs=rev if rev is None else [rev],
            targets=targets,
            deps_only=deps,
            on_error="raise",
            stages=stages,
        )
        params = {k: to_relpath(_repo.fs, _repo.root_dir, v) for k, v in params.items()}

    params = _postprocess(params)

    if not params:
        return {}

    return params[first(params)]

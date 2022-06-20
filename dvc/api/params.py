from collections import Counter
from typing import Dict, Iterable, Optional, Union

from funcy import first

from dvc.repo import Repo


def params_show(
    *targets: str,
    repo: Optional[str] = None,
    stages: Optional[Union[str, Iterable[str]]] = None,
    rev: Optional[str] = None,
    deps: bool = False,
) -> Dict:
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
    if isinstance(stages, str):
        stages = [stages]

    def _onerror_raise(result: Dict, exception: Exception, *args, **kwargs):
        raise exception

    def _postprocess(params):
        processed = {}
        for rev, rev_data in params.items():
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

        return processed[first(processed)]

    with Repo.open(repo) as _repo:
        params = _repo.params.show(
            revs=rev if rev is None else [rev],
            targets=targets,
            deps=deps,
            onerror=_onerror_raise,
            stages=stages,
        )

    return _postprocess(params)

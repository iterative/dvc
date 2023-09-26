from typing import Dict, List, Optional, Union

from rich.text import Text

from dvc.repo import Repo
from dvc.repo.experiments.show import tabulate


def exp_save(
    name: Optional[str] = None,
    force: bool = False,
    include_untracked: Optional[List[str]] = None,
):
    """
    Create a new DVC experiment using `exp save`.

    See https://dvc.org/doc/command-reference/exp/save.

    Args:
        name (str, optional): specify a name for this experiment.
            If `None`, a default one will be generated, such as `urban-sign`.
            Defaults to `None`.
        force (bool):  overwrite the experiment if an experiment with the same
            name already exists.
            Defaults to `False`.
        include_untracked (List[str], optional): specify untracked file(s) to
            be included in the saved experiment.
            Defaults to `None`.

    Returns:
        str: The `Git revision`_ of the created experiment.

    Raises:
        ExperimentExistsError: If an experiment with `name` already exists and
            `force=False`.

    .. _Git revision:
        https://git-scm.com/docs/revisions
    """
    with Repo() as repo:
        return repo.experiments.save(
            name=name, force=force, include_untracked=include_untracked
        )


def _postprocess(exp_rows):
    for exp_row in exp_rows:
        for k, v in exp_row.items():
            if isinstance(v, Text):
                v_str = str(v)
                try:
                    exp_row[k] = float(v_str)
                except ValueError:
                    exp_row[k] = v_str

            if not exp_row[k]:
                exp_row[k] = None

    return exp_rows


def exp_show(
    repo: Optional[str] = None,
    revs: Optional[Union[str, List[str]]] = None,
    num: int = 1,
    param_deps: bool = False,
    force: bool = False,
    config: Optional[Dict] = None,
) -> List[Dict]:
    """Get DVC experiments tracked in `repo`.

    Without arguments, this function will retrieve all experiments derived from
    the Git `HEAD`.

    See the options below to customize the experiments retrieved.

    Args:
        repo (str, optional): location of the DVC repository.
            Defaults to the current project (found by walking up from the
            current working directory tree).
            It can be a URL or a file system path.
            Both HTTP and SSH protocols are supported for online Git repos
            (e.g. [user@]server:project.git).
        revs (Union[str, List[str]], optional): Git revision(s) (e.g. branch,
            tag, SHA commit) to use as a reference point to start listing
            experiments.
            Defaults to `None`, which will use `HEAD` as starting point.
        num (int, optional): show experiments from the last `num` commits
            (first parents) starting from the `revs` baseline.
            Give a negative value to include all first-parent commits (similar
            to `git log -n`).
            Defaults to 1.
        param_deps (bool, optional): include only parameters that are stage
            dependencies.
            Defaults to `False`.
        force (bool, optional): force re-collection of experiments instead of
            loading from internal experiments cache.
            DVC caches `exp_show` data for completed experiments to improve
            performance of subsequent calls.
            When `force` is specified, DVC will reload all experiment data and
            ignore any previously cached results.
            Defaults to `False`.
        config (dict, optional): config to be passed through to DVC project.
            Defaults to `None`.

    Returns:
        List[Dict]: Each item in the list will contain a dictionary with
            the info for an individual experiment.
            See Examples below.
    """
    with Repo.open(repo, config=config) as _repo:
        experiments = _repo.experiments.show(
            revs=revs,
            num=num,
            param_deps=param_deps,
            force=force,
        )
        td, _ = tabulate(experiments, fill_value=None)

        return _postprocess(td.as_dict())

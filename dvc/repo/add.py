import logging
import os
from contextlib import contextmanager
from itertools import tee
from typing import TYPE_CHECKING, Any, Iterator, List, NamedTuple, Optional

import colorama

from dvc.exceptions import (
    CacheLinkError,
    DvcException,
    InvalidArgumentError,
    OutputDuplicationError,
    OutputNotFoundError,
    OverlappingOutputPathsError,
    RecursiveAddingWhileUsingFilename,
)
from dvc.repo.scm_context import scm_context
from dvc.ui import ui
from dvc.utils import LARGE_DIR_SIZE, glob_targets, resolve_output, resolve_paths
from dvc.utils.collections import ensure_list, validate

from . import locked

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.stage import Stage
    from dvc.types import TargetType

Stages = List["Stage"]
logger = logging.getLogger(__name__)


class StageInfo(NamedTuple):
    stage: "Stage"
    output_exists: bool


OVERLAPPING_CHILD_FMT = (
    "Cannot add '{out}', because it is overlapping with other "
    "DVC tracked output: '{parent}'.\n"
    "To include '{out}' in '{parent}', run "
    "'dvc commit {parent_stage}'"
)

OVERLAPPING_PARENT_FMT = (
    "Cannot add '{parent}', because it is overlapping with other "
    "DVC tracked output: '{out}'.\n"
    "To include '{out}' in '{parent}', run "
    "'dvc remove {out_stage}' and then 'dvc add {parent}'"
)


def check_recursive_and_fname(args):
    if args.recursive and args.fname:
        raise RecursiveAddingWhileUsingFilename()


def transform_targets(args):
    from funcy import count_reps

    counts = count_reps(ensure_list(args.targets))
    dupes = [key for key, count in counts.items() if count > 1]
    if dupes:
        msg = ", ".join(f"[b]{key}[/]" for key in dupes)
        ui.error_write(f"ignoring duplicated targets: {msg}", styled=True)
    args.targets = list(counts)


def check_arg_combinations(args):
    kwargs = args.kwargs
    invalid_opt = None
    to_remote = args.to_remote
    to_cache = kwargs.get("out") and not to_remote

    if to_remote or to_cache:
        message = "{option} can't be used with "
        message += "--to-remote" if to_remote else "-o"
        if len(args.targets) != 1:
            invalid_opt = "multiple targets"
        elif args.no_commit:
            invalid_opt = "--no-commit option"
        elif args.recursive:
            invalid_opt = "--recursive option"
        elif kwargs.get("external"):
            invalid_opt = "--external option"
    else:
        message = "{option} can't be used without --to-remote"
        if kwargs.get("remote"):
            invalid_opt = "--remote"
        elif kwargs.get("jobs"):
            invalid_opt = "--jobs"

    if invalid_opt is not None:
        raise InvalidArgumentError(message.format(option=invalid_opt))


@contextmanager
def translate_graph_error(stages: Stages) -> Iterator[None]:
    try:
        yield
    except OverlappingOutputPathsError as exc:
        if exc.parent in [o for s in stages for o in s.outs]:
            msg = OVERLAPPING_PARENT_FMT.format(
                out=exc.overlapping_out,
                parent=exc.parent,
                out_stage=exc.overlapping_out.stage.addressing,
            )
        else:
            msg = OVERLAPPING_CHILD_FMT.format(
                out=exc.overlapping_out,
                parent=exc.parent,
                parent_stage=exc.parent.stage.addressing,
            )
        raise OverlappingOutputPathsError(  # noqa: B904
            exc.parent, exc.overlapping_out, msg
        )
    except OutputDuplicationError as exc:
        raise OutputDuplicationError(  # noqa: B904
            exc.output, list(set(exc.stages) - set(stages))
        )


def progress_iter(stages: List[StageInfo]) -> Iterator["StageInfo"]:
    total = len(stages)
    desc = "Adding..."
    with ui.progress(stages, total=total, desc=desc, unit="file", leave=True) as pbar:
        if total == 1:
            pbar.bar_format = desc
            pbar.refresh()

        for item in pbar:
            if total > 1:
                pbar.set_msg(f"{item.stage.outs[0]}")
            yield item
            if total == 1:  # restore bar format for stats
                # pylint: disable=no-member
                pbar.bar_format = pbar.BAR_FMT_DEFAULT


LINK_FAILURE_MESSAGE = (
    "\nSome targets could not be linked from cache to workspace.\n{}\n"
    "To re-link these targets, reconfigure cache types and then run:\n"
    "\n\tdvc checkout {}"
)


@contextmanager
def warn_link_failures() -> Iterator[List[str]]:
    link_failures: List[str] = []
    try:
        yield link_failures
    finally:
        if link_failures:
            msg = LINK_FAILURE_MESSAGE.format(
                CacheLinkError.SUPPORT_LINK,
                " ".join(link_failures),
            )
            ui.error_write(msg)


VALIDATORS = (
    check_recursive_and_fname,
    transform_targets,
    check_arg_combinations,
)


@validate(*VALIDATORS)
@locked
@scm_context
def add(
    repo: "Repo",
    targets: "TargetType",
    recursive: bool = False,
    no_commit: bool = False,
    fname: Optional[str] = None,
    to_remote: bool = False,
    **kwargs: Any,
):
    to_cache = bool(kwargs.get("out")) and not to_remote
    transfer = to_remote or to_cache

    glob = kwargs.get("glob", False)
    add_targets = collect_targets(repo, targets, recursive, glob)
    # pass one for creating stages, other one is used for iterating here
    add_targets, sources = tee(add_targets)

    # collect targets and build stages as we go
    desc = "Collecting targets"
    stages_it = create_stages(repo, add_targets, fname, transfer, **kwargs)
    stages = list(ui.progress(stages_it, desc=desc, unit="file"))

    stages_list = [stage for stage, _ in stages]
    msg = "Collecting stages from the workspace"
    with translate_graph_error(stages_list), ui.status(msg) as status:
        # remove existing stages that are to-be replaced with these
        # new stages for the graph checks.
        repo.check_graph(
            stages=stages_list, callback=lambda: status.update("Checking graph")
        )

    odb = None
    if to_remote:
        odb = repo.cloud.get_remote_odb(kwargs.get("remote"), "add")

    with warn_link_failures() as link_failures:
        for (stage, output_exists), source in zip(progress_iter(stages), sources):
            out = stage.outs[0]
            if to_remote or to_cache:
                stage.transfer(source, to_remote=to_remote, odb=odb, **kwargs)
            else:
                try:
                    path = out.fs.path.abspath(source) if output_exists else None
                    stage.add_outs(path, no_commit=no_commit)
                except CacheLinkError:
                    link_failures.append(str(stage.relpath))
            stage.dump()
    return stages_list


LARGE_DIR_RECURSIVE_ADD_WARNING = (
    "You are adding a large directory '{target}' recursively.\n"
    "Consider tracking it as a whole instead with "
    "`{cyan}dvc add {target}{nc}`."
)


def collect_targets(
    repo: "Repo",
    targets: "TargetType",
    recursive: bool = False,
    glob: bool = False,
) -> Iterator[str]:
    for target in glob_targets(ensure_list(targets), glob=glob):
        expanded_targets = _find_all_targets(repo, target, recursive=recursive)
        for index, path in enumerate(expanded_targets):
            if index == LARGE_DIR_SIZE:
                msg = LARGE_DIR_RECURSIVE_ADD_WARNING.format(
                    cyan=colorama.Fore.CYAN,
                    nc=colorama.Style.RESET_ALL,
                    target=target,
                )
                ui.error_write(msg)
            yield path


def _find_all_targets(
    repo: "Repo", target: str, recursive: bool = False
) -> Iterator[str]:
    from dvc.dvcfile import is_dvc_file

    if os.path.isdir(target) and recursive:
        files = repo.dvcignore.find(repo.fs, target)
        yield from (
            path
            for path in files
            if not repo.is_dvc_internal(path)
            if not is_dvc_file(path)
            if not repo.scm.belongs_to_scm(path)
            if not repo.scm.is_tracked(path)
        )
    else:
        yield target


def create_stages(
    repo: "Repo",
    targets: Iterator[str],
    fname: Optional[str] = None,
    transfer: bool = False,
    external: bool = False,
    force: bool = False,
    **kwargs: Any,
) -> Iterator[StageInfo]:
    for target in targets:
        if kwargs.get("out"):
            target = resolve_output(target, kwargs["out"], force=force)
        path, wdir, out = resolve_paths(
            repo, target, always_local=transfer and not kwargs.get("out")
        )

        try:
            (out_obj,) = repo.find_outs_by_path(target, strict=False)
            stage = out_obj.stage
            if not stage.is_data_source:
                raise DvcException(f"cannot update {out!r}: not a data source")
            output_exists = True
        except OutputNotFoundError:
            stage = repo.stage.create(
                single_stage=True,
                validate=False,
                fname=fname or path,
                wdir=wdir,
                outs=[out],
                external=external,
                force=force,
            )
            output_exists = False

        out_obj = stage.outs[0]
        out_obj.annot.update(**kwargs)
        yield StageInfo(stage, output_exists)

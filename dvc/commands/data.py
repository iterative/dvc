import argparse
import logging
import os
from collections import defaultdict
from functools import partial
from operator import itemgetter

from funcy import log_durations

from dvc.cli.command import CmdBase
from dvc.cli.utils import fix_subparsers
from dvc.ui import ui

logger = logging.getLogger(__name__)

print_durations = partial(
    log_durations,
    ui.error_write
    if logger.isEnabledFor(logging.TRACE)  # type: ignore[attr-defined]
    else logger.trace,  # type: ignore[attr-defined]
)


class CmdDataStatus(CmdBase):
    def _process_data(
        self,
        ls_data,
        status_data,
        diff_data,
        git_staged,
        git_unstaged,
        git_untracked,
    ):
        files = set(map(itemgetter("path"), ls_data))
        ret = defaultdict(list)

        stage_modified = set()
        not_in_cache = set()
        for _, stage_status in status_data.items():
            for out_stats in stage_status:
                if isinstance(out_stats, dict):
                    for _, stats in out_stats.items():
                        if isinstance(stats, dict):
                            for path, typ in stats.items():
                                if typ == "modified":
                                    stage_modified.add(path)
                                if typ == "not in cache":
                                    not_in_cache.add(path)

        diff_type_map = {
            "modified": "modified_against_head",
            "added": "added",
            "deleted": "deleted",
            "renamed": "renamed",
        }
        diff_files = set()
        for typ, diff_p in diff_data.items():
            if typ not in diff_type_map:
                continue
            for info in diff_p:
                path = info["path"]
                if path not in stage_modified:
                    ret[diff_type_map[typ]].append(path)
                    diff_files.add(info["path"])

        ret.update(
            {
                "stage_modified": list(stage_modified),
                "not_in_cache": list(not_in_cache),
                "dvc_tracked": list(files),
                "git_staged": git_staged,
                "git_unstaged": list(git_unstaged),
                "git_untracked": list(git_untracked),
            }
        )
        return ret

    def _patch_clone(self):
        from funcy import monkey

        from dvc.scm import Git

        @monkey(Git, "clone")
        def clone(url, *args, **kwargs):
            with print_durations(f"cloning {os.path.basename(url)}"):
                return clone.original(url, *args, **kwargs)

    @print_durations()
    def run(self):
        from dvc.repo import lock_repo

        with print_durations("scm_status"):
            git_staged, git_unstaged, git_untracked = self.repo.scm.status()

        self._patch_clone()
        with lock_repo(self.repo):
            with print_durations("ls"):
                # pylint: disable=protected-access
                ls_data = self.repo._ls(recursive=True, dvc_only=True)
            with print_durations("status"):
                status_data = self.repo.status()
            with print_durations("diff"):
                diff_data = self.repo.diff()

        processed = self._process_data(
            ls_data,
            status_data,
            diff_data,
            git_staged,
            git_unstaged,
            git_untracked,
        )
        ui.write_json(processed)
        return 0


def add_parser(subparsers, parent_parser):
    data_parser = subparsers.add_parser(
        "data",
        parents=[parent_parser],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    data_subparsers = data_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc data CMD --help` to display command-specific help.",
    )
    fix_subparsers(data_subparsers)
    data_status_parser = data_subparsers.add_parser(
        "status",
        parents=[parent_parser],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    data_status_parser.add_argument(
        "--json", action="store_true", default=False
    )
    data_status_parser.set_defaults(func=CmdDataStatus)

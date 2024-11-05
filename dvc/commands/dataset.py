from typing import TYPE_CHECKING, Optional

from dvc.cli import formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import DvcException
from dvc.log import logger

if TYPE_CHECKING:
    from rich.text import Text

    from dvc.repo.datasets import Dataset, FileInfo

logger = logger.getChild(__name__)


def diff_files(old: list["FileInfo"], new: list["FileInfo"]) -> dict[str, list[str]]:
    old_files = {d.relpath: d for d in old}
    new_files = {d.relpath: d for d in new}
    rest = old_files.keys() & new_files.keys()
    return {
        "added": list(new_files.keys() - old_files.keys()),
        "deleted": list(old_files.keys() - new_files.keys()),
        "modified": [p for p in rest if new_files[p] != old_files[p]],
    }


class CmdDatasetAdd(CmdBase):
    @classmethod
    def display(cls, name: str, dataset: "Dataset", action: str = "Adding"):
        from dvc.ui import ui

        assert dataset.lock

        url = dataset.spec.url
        ver: str = ""
        if dataset.type == "dc":
            ver = f"v{dataset.lock.version}"
        if dataset.type == "dvc":
            if dataset.lock.path:
                url = f"{url}:/{dataset.lock.path.lstrip('/')}"
            if rev := dataset.lock.rev:
                ver = rev

        ver_part: Optional[Text] = None
        if ver:
            ver_part = ui.rich_text.assemble(" @ ", (ver, "repr.number"))
        text = ui.rich_text.assemble("(", (url, "repr.url"), ver_part or "", ")")
        ui.write(action, ui.rich_text(name, "cyan"), text, styled=True)

    def run(self):
        if not self.args.dvc and self.args.rev:
            raise DvcException("--rev can't be used without --dvc")
        if not self.args.dvc and self.args.path:
            raise DvcException("--path can't be used without --dvc")

        d = vars(self.args)
        for key in ["dvc", "dc", "url"]:
            if url := d.pop(key, None):
                d.update({"type": key, "url": url})
                break

        existing = self.repo.datasets.get(self.args.name)
        with self.repo.scm_context:
            if not self.args.force and existing:
                path = self.repo.fs.relpath(existing.manifest_path)
                raise DvcException(
                    f"{self.args.name} already exists in {path}, "
                    "use the --force to overwrite"
                )
            dataset = self.repo.datasets.add(**d)
            self.display(self.args.name, dataset)
            return 0


class CmdDatasetUpdate(CmdBase):
    def display(self, name: str, dataset: "Dataset", new: "Dataset"):
        from dvc.commands.checkout import log_changes
        from dvc.ui import ui

        action = "Updating"
        if not dataset.lock:
            return CmdDatasetAdd.display(name, new, action)
        if dataset == new:
            ui.write("[yellow]Nothing to update[/]", styled=True)
            return

        assert new.lock

        v: Optional[tuple[str, str]] = None
        if dataset.type == "dc":
            assert new.type == "dc"
            if new.lock.version < dataset.lock.version:
                action = "Downgrading"

            v = (f"v{dataset.lock.version}", f"v{new.lock.version}")
        if dataset.type == "dvc":
            assert new.type == "dvc"
            v = (f"{dataset.lock.rev_lock[:9]}", f"{new.lock.rev_lock[:9]}")

        if v:
            part = ui.rich_text.assemble(
                (v[0], "repr.number"),
                " -> ",
                (v[1], "repr.number"),
            )
        else:
            part = ui.rich_text(dataset.spec.url, "repr.url")
        changes = ui.rich_text.assemble("(", part, ")")
        ui.write(action, ui.rich_text(name, "cyan"), changes, styled=True)
        if dataset.type == "url":
            assert new.type == "url"
            stats = diff_files(dataset.lock.files, new.lock.files)
            log_changes(stats)

    def run(self):
        from difflib import get_close_matches

        from dvc.repo.datasets import DatasetNotFoundError
        from dvc.ui import ui

        version = None
        if self.args.rev:
            try:
                version = int(self.args.rev.lstrip("v"))
            except ValueError:
                version = self.args.rev

        d = vars(self.args) | {"version": version}
        with self.repo.scm_context:
            try:
                dataset, new = self.repo.datasets.update(**d)
            except DatasetNotFoundError:
                logger.exception("")
                if matches := get_close_matches(self.args.name, self.repo.datasets):
                    ui.write(
                        "did you mean?",
                        ui.rich_text(matches[0], "cyan"),
                        stderr=True,
                        styled=True,
                    )
                return 1
            self.display(self.args.name, dataset, new)
            return 0


def add_parser(subparsers, parent_parser):
    ds_parser = subparsers.add_parser(
        "dataset",
        aliases=["ds"],
        parents=[parent_parser],
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    ds_subparsers = ds_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc dataset CMD --help` to display command-specific help.",
        required=True,
    )

    dataset_add_help = "Add a dataset."
    ds_add_parser = ds_subparsers.add_parser(
        "add",
        parents=[parent_parser],
        description=append_doc_link(dataset_add_help, "dataset/add"),
        formatter_class=formatter.RawTextHelpFormatter,
        help=dataset_add_help,
    )

    url_exclusive_group = ds_add_parser.add_mutually_exclusive_group(required=True)
    url_exclusive_group.add_argument(
        "--dc", metavar="name", help="Name of the DataChain dataset to track"
    )
    url_exclusive_group.add_argument(
        "--dvc",
        help="Path or URL to a Git/DVC repository to track",
        metavar="url",
    )
    url_exclusive_group.add_argument(
        "--url",
        help="""\
URL of a cloud-versioned remote to track. Supported URLs:

s3://bucket/key/path
gs://bucket/path/to/file/or/dir
azure://mycontainer/path
remote://remote_name/path/to/file/or/dir (see `dvc remote`)
""",
    )
    ds_add_parser.add_argument("name", help="Name of the dataset to add")
    ds_add_parser.add_argument(
        "--rev",
        help="Git revision, e.g. SHA, branch, tag (only applicable with --dvc)",
        metavar="<commit>",
    )
    ds_add_parser.add_argument(
        "--path",
        help="Path to a file or a directory within a git repository "
        "(only applicable with --dvc)",
    )
    ds_add_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Overwrite existing dataset",
    )
    ds_add_parser.set_defaults(func=CmdDatasetAdd)

    dataset_update_help = "Update a dataset."
    ds_update_parser = ds_subparsers.add_parser(
        "update",
        parents=[parent_parser],
        description=append_doc_link(dataset_update_help, "dataset/update"),
        formatter_class=formatter.RawDescriptionHelpFormatter,
        help=dataset_update_help,
    )
    ds_update_parser.add_argument("name", help="Name of the dataset to update")
    ds_update_parser.add_argument(
        "--rev",
        "--version",
        nargs="?",
        help="DataChain dataset version or Git revision (e.g. SHA, branch, tag)",
        metavar="<version>",
    )
    ds_update_parser.set_defaults(func=CmdDatasetUpdate)

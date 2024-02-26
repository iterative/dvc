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
        if dataset.type == "dvcx":
            ver = f"v{dataset.lock.version}"
        if dataset.type == "dvc":
            if dataset.lock.path:
                url = f"{url}:/{dataset.lock.path.lstrip('/')}"
            if rev := dataset.lock.rev:
                ver = rev

        ver_part: Optional["Text"] = None
        if ver:
            ver_part = ui.rich_text.assemble(" @ ", (ver, "repr.number"))
        text = ui.rich_text.assemble("(", (url, "repr.url"), ver_part or "", ")")
        ui.write(action, ui.rich_text(name, "cyan"), text, styled=True)

    def run(self):
        existing = self.repo.datasets.get(self.args.name)
        with self.repo.scm_context:
            if not self.args.force and existing:
                path = self.repo.fs.relpath(existing.manifest_path)
                raise DvcException(
                    f"{self.args.name} already exists in {path}, "
                    "use the --force to overwrite"
                )
            dataset = self.repo.datasets.add(**vars(self.args))
            self.display(self.args.name, dataset)
            return 0


class CmdDatasetUpdate(CmdBase):
    def display(self, name: str, dataset: "Dataset", new: "Dataset"):
        from dvc.commands.checkout import log_changes
        from dvc.ui import ui

        if not dataset.lock:
            return CmdDatasetAdd.display(name, new, "Updating")
        if dataset == new:
            ui.write("[yellow]Nothing to update[/]", styled=True)
            return

        assert new.lock

        v: Optional[tuple[str, str]] = None
        if dataset.type == "dvcx":
            assert new.type == "dvcx"
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
        ui.write("Updating", ui.rich_text(name, "cyan"), changes, styled=True)
        if dataset.type == "url":
            assert new.type == "url"
            stats = diff_files(dataset.lock.files, new.lock.files)
            log_changes(stats)

    def run(self):
        from difflib import get_close_matches

        from dvc.repo.datasets import DatasetNotFoundError
        from dvc.ui import ui

        with self.repo.scm_context:
            try:
                dataset, new = self.repo.datasets.update(**vars(self.args))
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
    ds_add_parser.add_argument(
        "--url",
        required=True,
        help="""\
Location of the data to download. Supported URLs:

s3://bucket/key/path
gs://bucket/path/to/file/or/dir
azure://mycontainer/path
remote://remote_name/path/to/file/or/dir (see `dvc remote`)
dvcx://dataset_name

To import data from dvc/git repositories, \
add dvc:// schema to the repo url, e.g:
dvc://git@github.com/iterative/example-get-started.git
dvc+https://github.com/iterative/example-get-started.git""",
    )
    ds_add_parser.add_argument(
        "--name", help="Name of the dataset to add", required=True
    )
    ds_add_parser.add_argument(
        "--rev",
        help="Git revision, e.g. SHA, branch, tag "
        "(only applicable for dvc/git repository)",
    )
    ds_add_parser.add_argument(
        "--path", help="Path to a file or directory within the git repository"
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
        description=append_doc_link(dataset_update_help, "dataset/add"),
        formatter_class=formatter.RawDescriptionHelpFormatter,
        help=dataset_update_help,
    )
    ds_update_parser.add_argument("name", help="Name of the dataset to update")
    ds_update_parser.set_defaults(func=CmdDatasetUpdate)

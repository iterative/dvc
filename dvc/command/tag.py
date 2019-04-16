import logging

from dvc.utils import to_yaml_string
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase, fix_subparsers, append_doc_link


logger = logging.getLogger(__name__)


class CmdTagAdd(CmdBase):
    def run(self):
        for target in self.args.targets:
            try:
                self.repo.tag.add(
                    self.args.tag,
                    target=target,
                    with_deps=self.args.with_deps,
                    recursive=self.args.recursive,
                )
            except DvcException:
                logger.exception("failed to add tag")
                return 1
        return 0


class CmdTagRemove(CmdBase):
    def run(self):
        for target in self.args.targets:
            try:
                self.repo.tag.remove(
                    self.args.tag,
                    target=target,
                    with_deps=self.args.with_deps,
                    recursive=self.args.recursive,
                )
            except DvcException:
                logger.exception("failed to remove tag")
                return 1
        return 0


class CmdTagList(CmdBase):
    def run(self):
        for target in self.args.targets:
            try:
                tags = self.repo.tag.list(
                    target,
                    with_deps=self.args.with_deps,
                    recursive=self.args.recursive,
                )
                if tags:
                    logger.info(to_yaml_string(tags))
            except DvcException:
                logger.exception("failed list tags")
                return 1
        return 0


def add_parser(subparsers, parent_parser):
    TAG_HELP = "A set of commands to manage DVC tags."
    tag_parser = subparsers.add_parser(
        "tag",
        parents=[parent_parser],
        description=append_doc_link(TAG_HELP, "tag"),
        add_help=False,
    )

    tag_subparsers = tag_parser.add_subparsers(
        dest="cmd",
        help="Use DVC tag CMD --help to display command-specific help.",
    )

    fix_subparsers(tag_subparsers)

    TAG_ADD_HELP = "Add DVC tag."
    tag_add_parser = tag_subparsers.add_parser(
        "add",
        parents=[parent_parser],
        description=append_doc_link(TAG_ADD_HELP, "tag-add"),
        help=TAG_ADD_HELP,
    )
    tag_add_parser.add_argument("tag", help="Dvc tag.")
    tag_add_parser.add_argument(
        "targets", nargs="*", default=[None], help="Dvc files."
    )
    tag_add_parser.add_argument(
        "-d",
        "--with-deps",
        action="store_true",
        default=False,
        help="Add tag for all dependencies of the specified DVC file.",
    )
    tag_add_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Add tag for subdirectories of the specified directory.",
    )
    tag_add_parser.set_defaults(func=CmdTagAdd)

    TAG_REMOVE_HELP = "Remove DVC tag."
    tag_remove_parser = tag_subparsers.add_parser(
        "remove",
        parents=[parent_parser],
        description=append_doc_link(TAG_REMOVE_HELP, "tag-remove"),
        help=TAG_REMOVE_HELP,
    )
    tag_remove_parser.add_argument("tag", help="Dvc tag.")
    tag_remove_parser.add_argument(
        "targets", nargs="*", default=[None], help="Dvc files."
    )
    tag_remove_parser.add_argument(
        "-d",
        "--with-deps",
        action="store_true",
        default=False,
        help="Remove tag for all dependencies of the specified DVC file.",
    )
    tag_remove_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Remove tag for subdirectories of the specified directory.",
    )
    tag_remove_parser.set_defaults(func=CmdTagRemove)

    TAG_LIST_HELP = "List DVC tags."
    tag_list_parser = tag_subparsers.add_parser(
        "list",
        parents=[parent_parser],
        description=append_doc_link(TAG_LIST_HELP, "tag-list"),
        help=TAG_LIST_HELP,
    )
    tag_list_parser.add_argument(
        "targets", nargs="*", default=[None], help="Dvc files."
    )
    tag_list_parser.add_argument(
        "-d",
        "--with-deps",
        action="store_true",
        default=False,
        help="List tags for all dependencies of the specified DVC file.",
    )
    tag_list_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="List tags for subdirectories of the specified directory.",
    )
    tag_list_parser.set_defaults(func=CmdTagList)

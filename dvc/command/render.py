import argparse
import logging

from dvc.command.base import CmdBase, append_doc_link
from dvc.utils.serialize._yaml import dumps_yaml, render_dvc_template

logger = logging.getLogger(__name__)


class CmdRender(CmdBase):
    def run(self):
        with open(self.args.path, encoding="utf-8") as fd:
            text = fd.read()
        dvc_dict, vars_dict = render_dvc_template(text)
        vars_dict = {"vars": vars_dict}
        vars_str = dumps_yaml(vars_dict)

        if self.args.only_vars:
            logger.info(vars_str)
            return 0

        if self.args.stage is not None:
            dvc_dict = dvc_dict["stages"][self.args.stage]
        dvc_str = dumps_yaml(dvc_dict)

        if self.args.only_stages:
            logger.info(dvc_str)
            return 0

        logger.info("\n".join([vars_str, dvc_str]))
        return 0


def add_parser(subparsers, parent_parser):
    RENDER_HELP = "Render templated dvc.yaml"
    render_parser = subparsers.add_parser(
        "render",
        parents=[parent_parser],
        description=append_doc_link(RENDER_HELP, "render"),
        help=RENDER_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    render_parser.add_argument(
        "--path", default="dvc.yaml", help="Path to dvc.yaml file",
    )
    render_parser.add_argument(
        "--stage", "-s", default=None, help="Only render a specified stage",
    )
    render_parser.add_argument(
        "--only-vars",
        action="store_true",
        default=False,
        help="Only render the `vars` component of the dvc.yaml",
    )
    render_parser.add_argument(
        "--only-stages",
        action="store_true",
        default=False,
        help="Only render the `stages` component of the dvc.yaml",
    )

    render_parser.set_defaults(func=CmdRender)

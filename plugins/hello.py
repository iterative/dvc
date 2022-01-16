from argparse import Namespace
from dataclasses import dataclass

from dvc.hookspecs import hookimpl


@dataclass
class HelloCommand:
    args: Namespace

    def do_run(self):
        print("Hello, world!")
        return 0


@hookimpl
def register_command(parser, parent):
    hello_world = parser.add_parser(
        "hello", parents=[parent], help="print hello world"
    )
    hello_world.set_defaults(func=HelloCommand)

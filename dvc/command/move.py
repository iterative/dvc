import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdMove(CmdBase):
    def run(self):
        try:
            self.project.move(self.args.src, self.args.dst)
        except DvcException:
            msg = "failed to move '{}' -> '{}'".format(self.args.src,
                                                       self.args.dst)
            logger.error(msg)
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    MOVE_HELP = 'Move output of DVC file.'
    move_parser = subparsers.add_parser(
        'move',
        parents=[parent_parser],
        description=MOVE_HELP,
        help=MOVE_HELP)
    move_parser.add_argument('src', help='Source.')
    move_parser.add_argument('dst', help='Destination.')
    move_parser.set_defaults(func=CmdMove)

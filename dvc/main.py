from dvc.logger import Logger
from dvc.cli import parse_args
from dvc.command.base import CmdBase
from dvc.analytics import Analytics
from dvc.cli import DvcParserError
from dvc.project import NotDvcProjectError


def main(argv=None):
    Logger.init()

    args = None
    cmd = None
    try:
        args = parse_args(argv)

        # Init loglevel early in case we'll run
        # into errors before setting it properly
        CmdBase._set_loglevel(args)

        cmd = args.func(args)

        ret = cmd.run_cmd()
    except NotDvcProjectError as ex:
        Logger.error(str(ex))
        ret = 253
    except DvcParserError:
        ret = 254
    except Exception as ex:
        Logger.error('Unexpected error', ex)
        ret = 255

    Analytics().send_cmd(cmd, args, ret)

    return ret

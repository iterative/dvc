from dvc.logger import logger
from dvc.cli import parse_args
from dvc.command.base import CmdBase
from dvc.analytics import Analytics
from dvc.exceptions import NotDvcProjectError, DvcParserError


def main(argv=None):
    args = None
    cmd = None
    try:
        args = parse_args(argv)

        # Init loglevel early in case we'll run
        # into errors before setting it properly
        CmdBase._set_loglevel(args)

        cmd = args.func(args)

        ret = cmd.run_cmd()
    except KeyboardInterrupt as ex:
        logger.error("Interrupted by the user", ex)
        ret = 252
    except NotDvcProjectError as ex:
        logger.error("", ex)
        ret = 253
    except DvcParserError:
        ret = 254
    except Exception as ex:
        logger.error('Unexpected error', ex)
        ret = 255

    Analytics().send_cmd(cmd, args, ret)

    return ret

"""Main entry point for dvc CLI."""

from __future__ import unicode_literals

import dvc.logger as logger
from dvc.cli import parse_args
from dvc.command.base import CmdBase
from dvc.analytics import Analytics
from dvc.exceptions import NotDvcRepoError, DvcParserError


def main(argv=None):
    """Run dvc CLI command.

    Args:
        argv: optional list of arguments to parse. sys.argv is used by default.

    Returns:
        int: command's return code.
    """
    args = None
    cmd = None
    try:
        args = parse_args(argv)

        # Init loglevel early in case we'll run
        # into errors before setting it properly
        CmdBase.set_loglevel(args)

        cmd = args.func(args)

        ret = cmd.run_cmd()
    except KeyboardInterrupt:
        logger.error("interrupted by the user")
        ret = 252
    except NotDvcRepoError:
        logger.error()
        ret = 253
    except DvcParserError:
        ret = 254
    except Exception:  # pylint: disable=broad-except
        logger.error("unexpected error")
        ret = 255

    Analytics().send_cmd(cmd, args, ret)

    return ret

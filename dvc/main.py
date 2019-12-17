"""Main entry point for dvc CLI."""
from __future__ import unicode_literals

import errno
import logging

from dvc import analytics
from dvc.cli import parse_args
from dvc.config import ConfigError
from dvc.exceptions import DvcParserError
from dvc.exceptions import NotDvcRepoError
from dvc.external_repo import clean_repos
from dvc.logger import FOOTER
from dvc.remote.pool import close_pools
from dvc.utils.compat import is_py2


# Workaround for CPython bug. See [1] and [2] for more info.
# [1] https://github.com/aws/aws-cli/blob/1.16.277/awscli/clidriver.py#L55
# [2] https://bugs.python.org/issue29288
"".encode("idna")


logger = logging.getLogger("dvc")


def main(argv=None):
    """Run dvc CLI command.

    Args:
        argv: optional list of arguments to parse. sys.argv is used by default.

    Returns:
        int: command's return code.
    """
    args = None
    cmd = None

    outerLogLevel = logger.level
    try:
        args = parse_args(argv)

        if args.quiet:
            logger.setLevel(logging.CRITICAL)

        elif args.verbose:
            logger.setLevel(logging.DEBUG)

        cmd = args.func(args)
        ret = cmd.run()
    except ConfigError:
        logger.exception("configuration error")
        ret = 251
    except KeyboardInterrupt:
        logger.exception("interrupted by the user")
        ret = 252
    except NotDvcRepoError:
        logger.exception("")
        ret = 253
    except DvcParserError:
        ret = 254
    except Exception as exc:  # pylint: disable=broad-except
        if isinstance(exc, UnicodeError) and is_py2:
            logger.exception(
                "unicode is not supported in DVC for Python 2 "
                "(end-of-life January 1, 2020), please upgrade to Python 3"
            )
        elif isinstance(exc, OSError) and exc.errno == errno.EMFILE:
            logger.exception(
                "too many open files, please increase your `ulimit`"
            )
        else:
            logger.exception("unexpected error")
        ret = 255
    finally:
        logger.setLevel(outerLogLevel)

        # Python 2 fails to close these clean occasionally and users see
        # weird error messages, so we do it manually
        close_pools()

        # Remove cached repos in the end of the call, these are anonymous
        # so won't be reused by any other subsequent run anyway.
        clean_repos()

    if ret != 0:
        logger.info(FOOTER)

    if analytics.is_enabled():
        analytics.collect_and_send_report(args, ret)

    return ret

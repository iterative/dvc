"""
main entry point / argument parsing for dvc
"""

import sys
from dvc.settings import Settings
from dvc.logger import Logger
from dvc.config import ConfigError

def main():
    try:
        settings = Settings(sys.argv[1:])
        instance = settings._parsed_args.func(settings)
    except Exception as e:
        # In case we didn't even manage to parse options
        exc_info = '-v' in sys.argv or '--verbose' in sys.argv
        Logger.error("Settings error: {}".format(e), exc_info=exc_info)
        return 255

    try:
        ret = instance.run_cmd()
    except Exception as e:
        exc_info = settings.parsed_args.verbose
        Logger.error("{} error: {}".format(instance.__class__.__name__, e), exc_info=exc_info)
        return 254

    return ret

"""
main entry point / argument parsing for dvc
"""

import sys
from dvc.settings import Settings

def main():
    try:
        settings = Settings(sys.argv[1:])
        instance = settings._parsed_args.func(settings)
    except Exception as e:
        Logger.error("Exception caught while parsing settings", exc_info=True)
        return 255

    try:
        ret = instance.run()
    except Exception as e:
        Logger.error("Exception caught in " + instance.__class__.__name__, exc_info=True)
        return 254

    return ret

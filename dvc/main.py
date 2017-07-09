"""
main entry point / argument parsing for dvc
"""

import sys
from dvc.settings import Settings

def main():
    settings = Settings(sys.argv[1:])
    instance = settings._parsed_args.func(settings)
    sys.exit(instance.run())

"""Main entry point for dvc command line tool."""

from __future__ import unicode_literals

import sys

from dvc.main import main

if __name__ == "__main__":
    main(sys.argv[1:])

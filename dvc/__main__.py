"""Main entry point for DVC command line tool."""
import sys

from dvc.main import main

__name__ == "__main__" and sys.exit(main(sys.argv[1:]))


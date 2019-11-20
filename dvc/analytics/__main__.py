import sys

from dvc.analytics import send


if __name__ == "__main__":
    report_fname = sys.argv[1]
    send(report_fname)

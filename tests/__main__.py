# Needs investigation, pylint was running tests
# Could be `pylint_pytest` plugin, skipping check for this file for now
# pylint: skip-file

import os
import sys
from subprocess import check_call

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO_ROOT)

os.putenv(
    "PATH", ":".join([os.path.join(REPO_ROOT, "bin"), os.getenv("PATH")])
)
os.putenv("DVC_HOME", REPO_ROOT)

params = " ".join(sys.argv[1:])

cmd = "pytest -v -n=4 --cov=dvc --durations=0 {params}".format(params=params)
check_call(cmd, shell=True)

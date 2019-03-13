import os
import sys
from subprocess import check_call


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO_ROOT)

os.putenv(
    "PATH", "{}:{}".format(os.path.join(REPO_ROOT, "bin"), os.getenv("PATH"))
)
os.putenv("DVC_HOME", REPO_ROOT)
os.putenv("DVC_TEST", "true")

if len(sys.argv) == 1:
    scope = "--all-modules"
else:
    scope = " ".join(sys.argv[1:])

cmd = (
    "nosetests -v --processes=4 --process-timeout=600 --cover-inclusive "
    "--cover-erase --cover-package=dvc --with-coverage --with-flaky "
    "--logging-clear-handlers "
    "{scope} ".format(scope=scope)
)
check_call(cmd, shell=True)

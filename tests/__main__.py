import os
import sys
from subprocess import check_call


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO_ROOT)

os.putenv(
    "PATH", "{}:{}".format(os.path.join(REPO_ROOT, "bin"), os.getenv("PATH"))
)
os.putenv("DVC_HOME", REPO_ROOT)

params = " ".join(sys.argv[1:])

cmd = (
    "py.test -v -n=4 --timeout=600 --timeout_method=thread --log-level=debug"
    " --cov=dvc {params} --durations=0".format(params=params)
)
check_call(cmd, shell=True)

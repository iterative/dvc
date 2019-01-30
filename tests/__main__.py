import os
from subprocess import check_call

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO_ROOT)

os.putenv("PATH", "{}:{}".format(os.path.join(REPO_ROOT, "bin"), os.getenv("PATH")))
os.putenv("DVC_HOME", REPO_ROOT)
os.putenv("DVC_TEST", "true")

cmd = (
    "nosetests -v --processes=-1 --process-timeout=500 --cover-inclusive "
    "--cover-erase --cover-package=dvc --with-coverage --all-modules"
)
check_call(cmd, shell=True)

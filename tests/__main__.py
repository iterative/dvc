import os
from subprocess import check_call

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO_ROOT)

os.putenv('PATH', '{}:{}'.format(os.path.join(REPO_ROOT, 'bin'), os.getenv('PATH')))
os.putenv('DVC_HOME', REPO_ROOT)

cmd = 'nosetests -v --process-timeout=200 --cover-inclusive --cover-erase --cover-package=dvc --with-coverage'
# NOTE: run on CRON in single process mode to avoid problems in coverage
# module for nose tests, where it sometimes doesn't parse .coveragerc.
if not (os.getenv('TRAVIS') == 'true' and os.getenv('TRAVIS_EVENT_TYPE') == 'cron'):
    cmd += ' --processes=-1'
check_call(cmd, shell=True)

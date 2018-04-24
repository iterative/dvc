import os
from subprocess import check_call

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO_ROOT)

os.putenv('PATH', '{}:{}'.format(os.path.join(REPO_ROOT, 'bin'), os.getenv('PATH')))
os.putenv('DVC_HOME', REPO_ROOT)

check_call('nosetests --cover-inclusive --cover-erase --cover-package=dvc --with-coverage', shell=True)

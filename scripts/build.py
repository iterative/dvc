import argparse
import pathlib
from subprocess import STDOUT, check_call

path = pathlib.Path(__file__).parent.absolute()
dvc = path.parent / "dvc"
pyinstaller = path / "pyinstaller"
innosetup = path / "innosetup"
fpm = path / "fpm"

parser = argparse.ArgumentParser()
parser.add_argument(
    "pkg", choices=["deb", "rpm", "osxpkg", "exe"], help="package type"
)
args = parser.parse_args()

(dvc / "utils" / "build.py").write_text(f'PKG = "{args.pkg}"')

check_call(
    ["python", "build.py"], cwd=pyinstaller, stderr=STDOUT,
)

if args.pkg == "exe":
    check_call(
        ["python", "build.py"], cwd=innosetup, stderr=STDOUT,
    )
else:
    check_call(
        ["python", "build.py", args.pkg], cwd=fpm, stderr=STDOUT,
    )

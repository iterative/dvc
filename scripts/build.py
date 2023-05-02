import argparse
import pathlib
import sys
from subprocess import STDOUT, check_call

path = pathlib.Path(__file__).parent.absolute()
dvc = path.parent / "dvc"
pyinstaller = path / "pyinstaller"
innosetup = path / "innosetup"
fpm = path / "fpm"

parser = argparse.ArgumentParser()
parser.add_argument("pkg", choices=["deb", "rpm", "osxpkg", "exe"], help="package type")
parser.add_argument("--sign-application", default=False, action="store_true")
parser.add_argument("--application-id")
parser.add_argument("--sign-installer", default=False, action="store_true")
parser.add_argument("--installer-id")
parser.add_argument("--notarize", default=False, action="store_true")
parser.add_argument("--apple-id-username")
parser.add_argument("--apple-id-password")
args = parser.parse_args()

(dvc / "utils" / "build.py").write_text(f'PKG = "{args.pkg}"')

if not (dvc / "_dvc_version.py").exists():
    raise Exception("no version info found")

check_call(
    ["python", "build.py"],
    cwd=pyinstaller,
    stderr=STDOUT,
)

if args.sign_application:
    if args.pkg != "osxpkg":
        raise NotImplementedError
    if not args.application_id:
        print("--sign-application requires --application-id")
        sys.exit(1)
    check_call(
        ["python", "sign.py", "--application-id", args.application_id],
        cwd=pyinstaller,
        stderr=STDOUT,
    )

if args.pkg == "exe":
    check_call(
        ["python", "build.py"],
        cwd=innosetup,
        stderr=STDOUT,
    )
else:
    check_call(
        ["python", "build.py", args.pkg],
        cwd=fpm,
        stderr=STDOUT,
    )

if args.sign_installer:
    if args.pkg != "osxpkg":
        raise NotImplementedError
    if not all([args.installer_id, args.apple_id_username, args.apple_id_password]):
        print("--sign-installer requires --installer-id")
        sys.exit(1)
    check_call(
        ["python", "sign.py", "--installer-id", args.installer_id],
        cwd=fpm,
        stderr=STDOUT,
    )

if args.notarize:
    if args.pkg != "osxpkg":
        raise NotImplementedError
    if not all([args.apple_id_username, args.apple_id_password]):
        print("--notarize requires --apple-id-username and --apple-id-password")
        sys.exit(1)
    check_call(
        [
            "python",
            "notarize.py",
            "--apple-id-username",
            args.apple_id_username,
            "--apple-id-password",
            args.apple_id_password,
        ],
        cwd=fpm,
        stderr=STDOUT,
    )

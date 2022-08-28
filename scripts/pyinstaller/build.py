import os
import pathlib
from subprocess import STDOUT, check_call, check_output

path = pathlib.Path(__file__).parent.absolute()
hooks = path / "hooks"
dvc = path.parent.parent / "dvc"
entry = dvc / "__main__.py"

check_call(
    [
        "pyinstaller",
        "--additional-hooks-dir",
        os.fspath(hooks),
        "--name",
        "dvc",
        "-y",
        os.fspath(entry),
    ],
    cwd=path,
    stderr=STDOUT,
)

out = check_output(
    [
        path / "dist" / "dvc" / "dvc",
        "doctor",
    ],
    stderr=STDOUT,
).decode()

remotes = [
    "s3",
    "oss",
    "gdrive",
    "gs",
    "hdfs",
    "http",
    "webhdfs",
    "azure",
    "ssh",
    "webdav",
]

print(out)
for remote in remotes:
    assert f"\t{remote}" in out, f"Missing support for {remote}"

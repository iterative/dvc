import os
import shutil
import stat
import sys
from dataclasses import dataclass

import pytest

from dvc.cli import DvcParserError, main, parse_args
from dvc.commands.external import CmdExternal
from dvc.utils.fs import path_isin
from dvc.utils.strictyaml import make_relpath


def append_paths(*paths, env="PATH"):
    if env in os.environ:
        paths = [*paths, os.environ[env]]
    return os.pathsep.join(paths)


@dataclass
class ScriptInfo:
    executable: str
    script: str


def make_executable_path(path):
    # On Windows, if the executable path is in cwd, shutil.which returns
    # ".\\rel" paths, otherwise, it returns an absolute path.
    # On other platforms, it's always an absolute path.
    abspath = os.path.abspath(path)
    if os.name == "nt" and path_isin(abspath, os.getcwd()):
        return make_relpath(path)
    return abspath


@pytest.fixture
def make_external_cmd(tmp_dir, monkeypatch):
    def make(cmd: str, contents: str) -> ScriptInfo:
        monkeypatch.setenv("PATH", append_paths(str(tmp_dir)))

        contents = f"#!{sys.executable}\n{contents}\n"
        if os.name == "nt":
            # Here, we create a batch file that invokes python script.
            # `.BAT` files are usually in PATHEXT, but adding them explicitly
            # just in case.
            # We can just run this batch file without it's extension suffix
            # later, we can do so after adding particular extension to PATHEXT.
            monkeypatch.setenv("PATHEXT", append_paths(".BAT", "PATHEXT"))
            script = tmp_dir / f"{cmd}.py"
            script.write_text(contents, encoding="utf-8")

            executable = script.with_suffix(".BAT")
            executable.write_text(
                f'@echo off\n"{sys.executable}" "{script}" %*\n'
            )
        else:
            # We create a script without any extensions here, unlike in Windows
            executable = script = tmp_dir / cmd
            script.write_text(contents, encoding="utf-8")
            script.chmod(script.stat().st_mode | stat.S_IEXEC)

        assert shutil.which(cmd) == make_executable_path(executable)
        return ScriptInfo(str(executable), str(script))

    return make


def test_parse_args_unknown_subcommand(caplog):
    with pytest.raises(DvcParserError):
        parse_args(["-v", "ext", "target", "--out", "out"])
    assert "argument COMMAND: invalid choice: 'ext'" in caplog.text


def test_parse_args(make_external_cmd):
    cmd = make_external_cmd("dvc-ext", 'print("hello world!")')

    ret = parse_args(["-v", "ext", "target", "--out", "out"])
    assert ret.func is CmdExternal
    assert ret.args == ["target", "--out", "out"]
    assert ret.cmd == "ext"
    assert ret.executable == make_executable_path(cmd.executable)
    assert ret.verbose  # this should not be passed to cmd


def test_external_cmd(make_external_cmd, caplog, capfd):
    code = "import sys; print(sys.argv); sys.exit(1)"
    cmd = make_external_cmd("dvc-ext", code)

    assert main(["ext", "target", "--out", "out"]) == 1
    assert "exec: dvc-ext target --out out" in caplog.text

    output = capfd.readouterr()[0].strip().replace(r"\\", "\\")
    assert output == rf"['{cmd.script}', 'target', '--out', 'out']"

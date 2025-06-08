import json

from dvc.cli import parse_args
from dvc.commands.ls import CmdList, show_tree


def _test_cli(mocker, *args):
    cli_args = parse_args(["list", *args])
    assert cli_args.func == CmdList

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.ls")

    assert cmd.run() == 0
    return m


def test_list(mocker):
    url = "local_dir"
    m = _test_cli(mocker, url)
    m.assert_called_once_with(
        url,
        None,
        recursive=False,
        rev=None,
        dvc_only=False,
        config=None,
        remote=None,
        remote_config=None,
        maxdepth=None,
    )


def test_list_recursive(mocker):
    url = "local_dir"
    m = _test_cli(mocker, url, "-R")
    m.assert_called_once_with(
        url,
        None,
        recursive=True,
        rev=None,
        dvc_only=False,
        config=None,
        remote=None,
        remote_config=None,
        maxdepth=None,
    )


def test_list_git_ssh_rev(mocker):
    url = "git@github.com:repo"
    m = _test_cli(mocker, url, "--rev", "123")
    m.assert_called_once_with(
        url,
        None,
        recursive=False,
        rev="123",
        dvc_only=False,
        config=None,
        remote=None,
        remote_config=None,
        maxdepth=None,
    )


def test_list_targets(mocker):
    url = "local_dir"
    target = "subdir"
    m = _test_cli(mocker, url, target)
    m.assert_called_once_with(
        url,
        target,
        recursive=False,
        rev=None,
        dvc_only=False,
        config=None,
        remote=None,
        remote_config=None,
        maxdepth=None,
    )


def test_list_outputs_only(mocker):
    url = "local_dir"
    m = _test_cli(mocker, url, None, "--dvc-only")
    m.assert_called_once_with(
        url,
        None,
        recursive=False,
        rev=None,
        dvc_only=True,
        config=None,
        remote=None,
        remote_config=None,
        maxdepth=None,
    )


def test_list_config(mocker):
    url = "local_dir"
    m = _test_cli(
        mocker,
        url,
        None,
        "--config",
        "myconfig",
        "--remote",
        "myremote",
        "--remote-config",
        "k1=v1",
        "k2=v2",
    )
    m.assert_called_once_with(
        url,
        None,
        recursive=False,
        rev=None,
        dvc_only=False,
        config="myconfig",
        remote="myremote",
        remote_config={"k1": "v1", "k2": "v2"},
        maxdepth=None,
    )


def test_list_level(mocker):
    url = "local_dir"
    m = _test_cli(mocker, url, None, "--level", "1")
    m.assert_called_once_with(
        url,
        None,
        recursive=False,
        rev=None,
        dvc_only=False,
        config=None,
        remote=None,
        remote_config=None,
        maxdepth=1,
    )


def test_list_tree(mocker):
    url = "git@github.com:repo"
    cli_args = parse_args(
        [
            "list",
            url,
            "local_dir",
            "--rev",
            "123",
            "--tree",
            "--show-hash",
            "--size",
            "--level",
            "2",
            "--dvc-only",
            "--config",
            "myconfig",
            "--remote",
            "myremote",
            "--remote-config",
            "k1=v1",
            "k2=v2",
        ]
    )
    assert cli_args.func == CmdList

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.ls.ls_tree")

    assert cmd.run() == 0
    m.assert_called_once_with(
        url,
        "local_dir",
        rev="123",
        dvc_only=True,
        config="myconfig",
        remote="myremote",
        remote_config={"k1": "v1", "k2": "v2"},
        maxdepth=2,
    )


def test_show_json(mocker, capsys):
    cli_args = parse_args(["list", "local_dir", "--json"])
    assert cli_args.func == CmdList

    cmd = cli_args.func(cli_args)

    result = [{"key": "val"}]
    mocker.patch("dvc.repo.Repo.ls", return_value=result)

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    assert json.dumps(result) in out


def test_show_colors(mocker, capsys, monkeypatch):
    cli_args = parse_args(["list", "local_dir"])
    assert cli_args.func == CmdList
    cmd = cli_args.func(cli_args)

    monkeypatch.setenv("LS_COLORS", "ex=01;32:rs=0:di=01;34:*.xml=01;31:*.dvc=01;33:")
    result = [
        {"isdir": False, "isexec": 0, "isout": False, "path": ".dvcignore"},
        {"isdir": False, "isexec": 0, "isout": False, "path": ".gitignore"},
        {"isdir": False, "isexec": 0, "isout": False, "path": "README.md"},
        {"isdir": True, "isexec": 0, "isout": True, "path": "data"},
        {"isdir": False, "isexec": 0, "isout": True, "path": "structure.xml"},
        {"isdir": False, "isexec": 0, "isout": False, "path": "structure.xml.dvc"},
        {"isdir": True, "isexec": 0, "isout": False, "path": "src"},
        {"isdir": False, "isexec": 1, "isout": False, "path": "run.sh"},
    ]
    mocker.patch("dvc.repo.Repo.ls", return_value=result)

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    entries = out.splitlines()

    assert entries == [
        ".dvcignore",
        ".gitignore",
        "README.md",
        "\x1b[01;34mdata\x1b[0m",
        "\x1b[01;31mstructure.xml\x1b[0m",
        "\x1b[01;33mstructure.xml.dvc\x1b[0m",
        "\x1b[01;34msrc\x1b[0m",
        "\x1b[01;32mrun.sh\x1b[0m",
    ]


def test_show_size(mocker, capsys):
    cli_args = parse_args(["list", "local_dir", "--size"])
    assert cli_args.func == CmdList
    cmd = cli_args.func(cli_args)

    result = [
        {
            "isdir": False,
            "isexec": 0,
            "isout": False,
            "path": ".dvcignore",
            "size": 100,
        },
        {
            "isdir": False,
            "isexec": 0,
            "isout": False,
            "path": ".gitignore",
            "size": 200,
        },
        {
            "isdir": False,
            "isexec": 0,
            "isout": False,
            "path": "README.md",
            "size": 100_000,
        },
    ]
    mocker.patch("dvc.repo.Repo.ls", return_value=result)

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    entries = out.splitlines()

    assert entries == ["  100  .dvcignore", "  200  .gitignore", "97.7k  README.md"]


def test_show_hash(mocker, capsys):
    cli_args = parse_args(["list", "local_dir", "--show-hash"])
    assert cli_args.func == CmdList
    cmd = cli_args.func(cli_args)

    result = [
        {
            "isdir": False,
            "isexec": 0,
            "isout": False,
            "path": ".dvcignore",
            "md5": "123",
        },
        {
            "isdir": False,
            "isexec": 0,
            "isout": False,
            "path": ".gitignore",
            "md5": "456",
        },
        {
            "isdir": False,
            "isexec": 0,
            "isout": False,
            "path": "README.md",
            "md5": "789",
        },
    ]
    mocker.patch("dvc.repo.Repo.ls", return_value=result)

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    entries = out.splitlines()

    assert entries == ["123  .dvcignore", "456  .gitignore", "789  README.md"]


def test_show_size_and_hash(mocker, capsys):
    cli_args = parse_args(["list", "local_dir", "--size", "--show-hash"])
    assert cli_args.func == CmdList
    cmd = cli_args.func(cli_args)

    result = [
        {
            "isdir": False,
            "isexec": 0,
            "isout": False,
            "path": ".dvcignore",
            "size": 100,
            "md5": "123",
        },
        {
            "isdir": False,
            "isexec": 0,
            "isout": False,
            "path": ".gitignore",
            "size": 200,
            "md5": "456",
        },
        {
            "isdir": False,
            "isexec": 0,
            "isout": False,
            "path": "README.md",
            "size": 100_000,
            "md5": "789",
        },
    ]
    mocker.patch("dvc.repo.Repo.ls", return_value=result)

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    entries = out.splitlines()

    assert entries == [
        "  100  123  .dvcignore",
        "  200  456  .gitignore",
        "97.7k  789  README.md",
    ]


def test_show_tree(capsys):
    entries = {
        "data": {
            "isout": True,
            "isdir": True,
            "isexec": False,
            "size": 192,
            "path": "data",
            "md5": "3fb071066d5d5b282f56a0169340346d.dir",
            "contents": {
                "dir": {
                    "isout": False,
                    "isdir": True,
                    "isexec": False,
                    "size": 96,
                    "path": "data/dir",
                    "md5": None,
                    "contents": {
                        "subdir": {
                            "isout": True,
                            "isdir": True,
                            "isexec": False,
                            "size": 96,
                            "md5": None,
                            "path": "data/dir/subdir",
                            "contents": {
                                "foobar": {
                                    "isout": True,
                                    "isdir": False,
                                    "isexec": False,
                                    "size": 4,
                                    "md5": "d3b07384d113edec49eaa6238ad5ff00",
                                }
                            },
                        },
                        "foo": {
                            "isout": False,
                            "isdir": False,
                            "isexec": False,
                            "size": 4,
                            "path": "data/dir/foo",
                            "md5": None,
                        },
                    },
                },
                "bar": {
                    "isout": True,
                    "isdir": False,
                    "isexec": False,
                    "size": 4,
                    "path": "data/bar",
                    "md5": "c157a79031e1c40f85931829bc5fc552",
                },
                "large-file": {
                    "isout": False,
                    "isdir": False,
                    "isexec": False,
                    "size": 1073741824,
                    "path": "data/large-file",
                    "md5": None,
                },
                "dir2": {
                    "isout": True,
                    "isdir": True,
                    "isexec": False,
                    "size": 96,
                    "md5": None,
                    "path": "data/dir2",
                    "contents": {
                        "foo": {
                            "isout": True,
                            "isdir": False,
                            "isexec": False,
                            "size": 4,
                            "path": "data/dir2/foo",
                            "md5": "d3b07384d113edec49eaa6238ad5ff00",
                        }
                    },
                },
            },
        }
    }

    show_tree(entries, with_color=False)
    out, _ = capsys.readouterr()
    expected = """\
data
├── dir
│   ├── subdir
│   │   └── foobar
│   └── foo
├── bar
├── large-file
└── dir2
    └── foo
"""
    assert out == expected

    show_tree(entries, with_color=False, with_size=True)
    out, _ = capsys.readouterr()
    expected = """\
  192  data
   96  ├── dir
   96  │   ├── subdir
    4  │   │   └── foobar
    4  │   └── foo
    4  ├── bar
1.00G  ├── large-file
   96  └── dir2
    4      └── foo
"""
    assert out == expected

    show_tree(entries, with_color=False, with_hash=True)
    out, _ = capsys.readouterr()
    expected = """\
3fb071066d5d5b282f56a0169340346d.dir  data
-                                     ├── dir
-                                     │   ├── subdir
d3b07384d113edec49eaa6238ad5ff00      │   │   └── foobar
-                                     │   └── foo
c157a79031e1c40f85931829bc5fc552      ├── bar
-                                     ├── large-file
-                                     └── dir2
d3b07384d113edec49eaa6238ad5ff00          └── foo
"""
    assert out == expected

    show_tree(entries, with_color=False, with_hash=True, with_size=True)
    out, _ = capsys.readouterr()
    expected = """\
  192  3fb071066d5d5b282f56a0169340346d.dir  data
   96  -                                     ├── dir
   96  -                                     │   ├── subdir
    4  d3b07384d113edec49eaa6238ad5ff00      │   │   └── foobar
    4  -                                     │   └── foo
    4  c157a79031e1c40f85931829bc5fc552      ├── bar
1.00G  -                                     ├── large-file
   96  -                                     └── dir2
    4  d3b07384d113edec49eaa6238ad5ff00          └── foo
"""
    assert out == expected


def test_list_alias():
    cli_args = parse_args(["ls", "local_dir"])
    assert cli_args.func == CmdList

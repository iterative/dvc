from dvc.command.ls.ls_colors import LsColors


def colorize(ls_colors):
    def _colorize(f, spec=""):
        fs_path = {
            "path": f,
            "isexec": "e" in spec,
            "isdir": "d" in spec,
            "isout": "o" in spec,
        }
        return ls_colors.format(fs_path)

    return _colorize


def test_ls_colors_out_file():
    ls_colors = LsColors(LsColors.default)
    assert colorize(ls_colors)("file", "o") == "file"


def test_ls_colors_out_dir():
    ls_colors = LsColors(LsColors.default)
    assert colorize(ls_colors)("dir", "do") == "\x1b[01;34mdir\x1b[0m"


def test_ls_colors_out_exec():
    ls_colors = LsColors(LsColors.default)
    assert (
        colorize(ls_colors)("script.sh", "eo") == "\x1b[01;32mscript.sh\x1b[0m"
    )


def test_ls_colors_out_ext():
    ls_colors = LsColors(LsColors.default + ":*.xml=01;33")
    assert colorize(ls_colors)("file.xml", "o") == "\x1b[01;33mfile.xml\x1b[0m"


def test_ls_colors_file():
    ls_colors = LsColors(LsColors.default)
    assert colorize(ls_colors)("file") == "file"


def test_ls_colors_dir():
    ls_colors = LsColors(LsColors.default)
    assert colorize(ls_colors)("dir", "d") == "\x1b[01;34mdir\x1b[0m"


def test_ls_colors_exec():
    ls_colors = LsColors(LsColors.default)
    assert (
        colorize(ls_colors)("script.sh", "e") == "\x1b[01;32mscript.sh\x1b[0m"
    )


def test_ls_colors_ext():
    ls_colors = LsColors(LsColors.default + ":*.xml=01;33")
    assert colorize(ls_colors)("file.xml") == "\x1b[01;33mfile.xml\x1b[0m"

from argparse import Namespace

import pytest
from funcy import raiser

from dvc.cli import main
from dvc_data.hashfile.build import IgnoreInCollectedDirError
from dvc_data.hashfile.cache import DiskError
from dvc_objects.fs.base import FileSystem, RemoteMissingDepsError


def test_state_pickle_errors_are_correctly_raised(tmp_dir, caplog, mocker):
    path = tmp_dir / "dir" / "test"
    mocker.patch(
        "dvc.cli.parse_args",
        return_value=Namespace(
            func=raiser(DiskError(path, "md5s")),
            quiet=False,
            verbose=True,
        ),
    )

    assert main() == 255
    assert (
        "Could not open pickled 'md5s' cache.\n"
        f"Remove the '{path.relative_to(tmp_dir)}' directory "
        "and then retry this command.\n"
        "See <https://error.dvc.org/pickle> for more information." in caplog.text
    )


@pytest.mark.parametrize(
    "pkg, msg",
    [
        (None, "Please report this bug to"),
        ("pip", "pip install 'dvc[proto]'"),
        ("conda", "conda install -c conda-forge dvc-proto"),
    ],
)
def test_remote_missing_deps_are_correctly_reported(tmp_dir, caplog, mocker, pkg, msg):
    error = RemoteMissingDepsError(FileSystem(), "proto", "proto://", ["deps"])
    mocker.patch("dvc.PKG", pkg)
    mocker.patch(
        "dvc.cli.parse_args",
        return_value=Namespace(func=raiser(error), quiet=False, verbose=True),
    )

    assert main() == 255
    expected = (
        "URL 'proto://' is supported but requires these missing dependencies: "
        "['deps']. "
    )
    if pkg:
        expected += (
            "To install dvc with those dependencies, run:\n\n"
            f"\t{msg}\n\n"
            "See <https://dvc.org/doc/install> for more info."
        )
    else:
        expected += (
            "\nPlease report this bug to "
            "<https://github.com/iterative/dvc/issues>. "
            "Thank you!"
        )
    assert expected in caplog.text


def test_ignore_in_collected_dir_error_is_logged(tmp_dir, caplog, mocker):
    error = IgnoreInCollectedDirError(".dvcignore", "dir")
    mocker.patch(
        "dvc.cli.parse_args",
        return_value=Namespace(func=raiser(error), quiet=False, verbose=True),
    )
    assert main() == 255
    expected = ".dvcignore file should not be in collected dir path: 'dir'"
    assert expected in caplog.text

from argparse import Namespace

from funcy import raiser

from dvc.cli import main
from dvc.objects.cache import DiskError


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
        f"Could not open pickled 'md5s' cache.\n"
        f"Remove the '{path.relative_to(tmp_dir)}' directory "
        "and then retry this command.\n"
        "See <https://error.dvc.org/pickle> for more information."
    ) in caplog.text

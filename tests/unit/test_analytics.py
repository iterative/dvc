from dvc.analytics import Analytics
from dvc.config import Config
from dvc.repo import NotDvcRepoError


def test_broken_config(mocker, dvc_repo):
    Config(dvc_repo.dvc_dir, validate=False).set("cache", "type", "unknown")

    assert Analytics._get_current_config()

    with mocker.patch(
        "dvc.repo.Repo.find_dvc_dir", side_effect=NotDvcRepoError(".")
    ):
        assert Analytics._get_current_config()

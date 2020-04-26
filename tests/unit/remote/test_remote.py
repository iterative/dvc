import pytest

from dvc.remote import Remote, S3Remote, GSRemote


def test_remote_with_checksum_jobs(dvc):
    dvc.config["remote"]["with_checksum_jobs"] = {
        "url": "s3://bucket/name",
        "checksum_jobs": 100,
    }
    dvc.config["core"]["checksum_jobs"] = 200

    remote = Remote(dvc, name="with_checksum_jobs")
    assert remote.checksum_jobs == 100


def test_remote_without_checksum_jobs(dvc):
    dvc.config["remote"]["without_checksum_jobs"] = {"url": "s3://bucket/name"}
    dvc.config["core"]["checksum_jobs"] = 200

    remote = Remote(dvc, name="without_checksum_jobs")
    assert remote.checksum_jobs == 200


def test_remote_without_checksum_jobs_default(dvc):
    dvc.config["remote"]["without_checksum_jobs"] = {"url": "s3://bucket/name"}

    remote = Remote(dvc, name="without_checksum_jobs")
    assert remote.checksum_jobs == remote.CHECKSUM_JOBS


@pytest.mark.parametrize("remote_cls", [GSRemote, S3Remote])
def test_makedirs_not_create_for_top_level_path(remote_cls, dvc, mocker):
    url = "{.scheme}://bucket/".format(remote_cls)
    remote = remote_cls(dvc, {"url": url})
    mocked_client = mocker.PropertyMock()
    # we use remote clients with same name as scheme to interact with remote
    mocker.patch.object(remote_cls, remote.scheme, mocked_client)

    remote.makedirs(remote.path_info)
    assert not mocked_client.called

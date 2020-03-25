import pytest

from dvc.remote import Remote, RemoteS3, RemoteGS


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


@pytest.mark.parametrize("remote_cls", [RemoteGS, RemoteS3])
def test_makedirs_not_create_for_top_level_path(remote_cls, mocker):
    url = "{.scheme}://bucket/".format(remote_cls)
    remote = remote_cls(None, {"url": url})
    exc = Exception("should not try to create dir on top-level path")
    client = mocker.PropertyMock(side_effect=exc)
    # we use remote clients with same name as scheme to interact with remote
    setattr(remote_cls, remote_cls.scheme, client)
    remote.makedirs(remote.path_info)

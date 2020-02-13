from dvc.remote import Remote


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

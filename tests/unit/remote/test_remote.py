from dvc.remote import Remote


def set_config_opts(dvc, commands):
    list(map(lambda args: dvc.config.set(*args), commands))


def test_remote_with_checksum_jobs(dvc):
    set_config_opts(
        dvc,
        [
            ('remote "with_checksum_jobs"', "url", "s3://bucket/name"),
            ('remote "with_checksum_jobs"', "checksum_jobs", 100),
            ("core", "checksum_jobs", 200),
        ],
    )

    remote = Remote(dvc, name="with_checksum_jobs")
    assert remote.checksum_jobs == 100


def test_remote_without_checksum_jobs(dvc):
    set_config_opts(
        dvc,
        [
            ('remote "without_checksum_jobs"', "url", "s3://bucket/name"),
            ("core", "checksum_jobs", "200"),
        ],
    )

    remote = Remote(dvc, name="without_checksum_jobs")
    assert remote.checksum_jobs == 200


def test_remote_without_checksum_jobs_default(dvc):
    set_config_opts(
        dvc, [('remote "without_checksum_jobs"', "url", "s3://bucket/name")]
    )

    remote = Remote(dvc, name="without_checksum_jobs")
    assert remote.checksum_jobs == remote.CHECKSUM_JOBS

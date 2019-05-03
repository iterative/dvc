from dvc.remote.local import RemoteLOCAL


def test_status_download_optimization(mocker):
    """When comparing the status to pull a remote cache,
        And the desired files to fetch are already on the local cache,
        Don't check the existance of the desired files on the remote cache
    """
    remote = RemoteLOCAL(None, {})

    checksum_infos = [
        {
            "path": "foo",
            "metric": False,
            "cache": True,
            "persist": False,
            "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
        },
        {
            "path": "bar",
            "metric": False,
            "cache": True,
            "persist": False,
            "md5": "37b51d194a7513e45b56f6524f2d51f2",
        },
    ]

    local_exists = [
        "acbd18db4cc2f85cedef654fccc4a4d8",
        "37b51d194a7513e45b56f6524f2d51f2",
    ]

    mocker.patch.object(remote, "cache_exists", return_value=local_exists)

    other_remote = mocker.Mock()
    other_remote.url = "other_remote"
    other_remote.cache_exists.return_value = []

    remote.status(checksum_infos, other_remote, download=True)

    assert other_remote.cache_exists.call_count == 0

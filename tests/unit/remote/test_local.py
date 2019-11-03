from dvc.cache import NamedCache
from dvc.remote.local import RemoteLOCAL


def test_status_download_optimization(mocker):
    """When comparing the status to pull a remote cache,
        And the desired files to fetch are already on the local cache,
        Don't check the existence of the desired files on the remote cache
    """
    remote = RemoteLOCAL(None, {})

    infos = NamedCache()
    infos.add("local", "acbd18db4cc2f85cedef654fccc4a4d8", "foo")
    infos.add("local", "37b51d194a7513e45b56f6524f2d51f2", "bar")

    local_exists = list(infos["local"])
    mocker.patch.object(remote, "cache_exists", return_value=local_exists)

    other_remote = mocker.Mock()
    other_remote.url = "other_remote"
    other_remote.cache_exists.return_value = []

    remote.status(infos, other_remote, download=True)

    assert other_remote.cache_exists.call_count == 0

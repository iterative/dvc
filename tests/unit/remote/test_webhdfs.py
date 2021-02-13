from dvc.fs.webhdfs import WebHDFSFileSystem

user = "test"
webhdfs_token = "token"
webhdfs_alias = "alias-name"
hdfscli_config = "path/to/cli/config"


def test_init(dvc):
    url = "webhdfs://test@127.0.0.1:50070"
    config = {
        "url": url,
        "webhdfs_token": webhdfs_token,
        "webhdfs_alias": webhdfs_alias,
        "hdfscli_config": hdfscli_config,
        "user": user,
    }

    fs = WebHDFSFileSystem(dvc, config)
    assert fs.path_info == url
    assert fs.token == webhdfs_token
    assert fs.alias == webhdfs_alias
    assert fs.path_info.user == user
    assert fs.hdfscli_config == hdfscli_config

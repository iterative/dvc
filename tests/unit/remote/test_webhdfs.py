import pytest
import requests
from dvc_webhdfs import WebHDFSFileSystem

host = "host"
kerberos = False
kerberos_principal = "principal"
port = 12345
proxy_to = "proxy"
ssl_verify = False
token = "token"
use_https = True
user = "test"


@pytest.fixture(name="webhdfs_config")
def fixture_webhdfs_config():
    url = f"webhdfs://{user}@{host}:{port}"
    url_config = WebHDFSFileSystem._get_kwargs_from_urls(url)
    return {
        "kerberos": kerberos,
        "kerberos_principal": kerberos_principal,
        "proxy_to": proxy_to,
        "ssl_verify": ssl_verify,
        "token": token,
        "use_https": use_https,
        **url_config,
    }


def test_init(dvc, webhdfs_config):
    fs = WebHDFSFileSystem(**webhdfs_config)
    assert fs.fs_args["host"] == host
    assert fs.fs_args["token"] == token
    assert fs.fs_args["user"] == user
    assert fs.fs_args["port"] == port
    assert fs.fs_args["kerberos"] == kerberos
    assert fs.fs_args["kerb_kwargs"] == {"principal": kerberos_principal}
    assert fs.fs_args["proxy_to"] == proxy_to
    assert fs.fs_args["use_https"] == use_https


def test_verify_ssl(dvc, webhdfs_config, monkeypatch, mocker):
    mock_session = mocker.create_autospec(requests.Session)
    monkeypatch.setattr(requests, "Session", mocker.Mock(return_value=mock_session))
    # can't have token at the same time as user or proxy_to
    del webhdfs_config["token"]
    fs = WebHDFSFileSystem(**webhdfs_config)
    # ssl verify can't be set until after the file system is instantiated
    assert fs.fs
    assert mock_session.verify == ssl_verify

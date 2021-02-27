import pytest

from dvc.exceptions import DvcException, HTTPError
from dvc.fs import Nexus3FileSystem

VALID_NEXUS3_URL = "nexus3://example.com/repository/repo/dvc_test"


def test_download_fails_on_error_code(dvc, nexus3):
    fs = Nexus3FileSystem(dvc, nexus3.config)

    with pytest.raises(HTTPError):
        fs._download(nexus3 / "missing.txt", "missing.txt")


def test_fails_on_no_repository_in_url(dvc):
    config = {"url": "nexus3://example.com/"}

    with pytest.raises(DvcException) as excinfo:
        Nexus3FileSystem(dvc, config)

    assert "Please specify a repository in your Nexus3 url" in str(
        excinfo.value
    )


def test_fails_on_no_project_directory_in_url(dvc):
    config = {"url": "nexus3://example.com/repository/repo"}

    with pytest.raises(DvcException) as excinfo:
        Nexus3FileSystem(dvc, config)

    assert "Please specify a folder in your Nexus3 url" in str(excinfo.value)


def test_public_auth_method(dvc):
    config = {
        "url": VALID_NEXUS3_URL,
        "path_info": "file.html",
        "user": "",
        "password": "",
    }

    fs = Nexus3FileSystem(dvc, config)

    assert fs._auth_method() is None


def test_basic_auth_method(dvc):
    from requests.auth import HTTPBasicAuth

    user = "username"
    password = "password"
    auth = HTTPBasicAuth(user, password)
    config = {
        "url": VALID_NEXUS3_URL,
        "path_info": "file.html",
        "auth": "basic",
        "user": user,
        "password": password,
    }

    fs = Nexus3FileSystem(dvc, config)

    assert fs._auth_method() == auth
    assert isinstance(fs._auth_method(), HTTPBasicAuth)


def test_ssl_verify_is_enabled_by_default(dvc):
    config = {
        "url": VALID_NEXUS3_URL,
        "path_info": "file.html",
    }

    fs = Nexus3FileSystem(dvc, config)

    assert fs._session.verify is True


def test_ssl_verify_disable(dvc):
    config = {
        "url": VALID_NEXUS3_URL,
        "path_info": "file.html",
        "ssl_verify": False,
    }

    fs = Nexus3FileSystem(dvc, config)

    assert fs._session.verify is False


def test_exists(mocker):
    import io

    import requests

    from dvc.path_info import URLInfo

    res = requests.Response()
    # need to add `raw`, as `exists()` fallbacks to a streaming GET requests
    # on HEAD request failure.
    res.raw = io.StringIO("foo")

    fs = Nexus3FileSystem(None, {})
    mocker.patch.object(fs, "request", return_value=res)

    url = URLInfo(f"{VALID_NEXUS3_URL}/file.txt")

    res.status_code = 200
    assert fs.exists(url) is True

    res.status_code = 404
    assert fs.exists(url) is False

    res.status_code = 403
    with pytest.raises(HTTPError):
        fs.exists(url)


@pytest.mark.parametrize(
    "test_url, hostname, repository, directory, filename",
    [
        (
            "nexus3://example.com/repository/repo/dvc_test/folder/filename",
            "https://example.com",
            "repo",
            "dvc_test/folder",
            "filename",
        ),
        (
            "nexus3://example.com/repository/repo/dvc_test/folder/folder2/",
            "https://example.com",
            "repo",
            "dvc_test/folder/folder2",
            "",
        ),
        (
            "nexus3://example.com/repository/repo/dvc_test/fo/lder"
            "/filename.txt",
            "https://example.com",
            "repo",
            "dvc_test/fo/lder",
            "filename.txt",
        ),
        (
            "nexus3://example.com/repository/samesame/samesame/samesame"
            "/samesame",
            "https://example.com",
            "samesame",
            "samesame/samesame",
            "samesame",
        ),
        (
            "nexus3://example.com/repository/repo/dvc_test/",
            "https://example.com",
            "repo",
            "dvc_test",
            "",
        ),
    ],
)
def test_extract_nexus_repo_info_from_url(
    dvc, test_url, hostname, repository, directory, filename
):
    config = {
        "url": VALID_NEXUS3_URL,
    }
    fs = Nexus3FileSystem(dvc, config)

    (
        _hostname,
        _repository,
        _directory,
        _filename,
    ) = fs.extract_nexus_repo_info_from_url(fs.PATH_CLS(test_url))
    assert _hostname == hostname
    assert _repository == repository
    assert _directory == directory
    assert _filename == filename


@pytest.mark.parametrize(
    "test_url, expected_url, unsecure",
    [
        (
            "nexus3://example.com/repository/repo/dvc_test/filename",
            "https://example.com/repository/repo/dvc_test/filename",
            False,
        ),
        (
            "https://example.com/repository/repo/dvc_test/filename",
            "https://example.com/repository/repo/dvc_test/filename",
            False,
        ),
        (
            "nexus3://example.com/repository/repo/dvc_test/filename",
            "http://example.com/repository/repo/dvc_test/filename",
            True,
        ),
    ],
)
def test_generate_download_url_without_auth(
    dvc, test_url, expected_url, unsecure
):
    config = {"url": VALID_NEXUS3_URL, "unsecure": unsecure}
    fs = Nexus3FileSystem(dvc, config)

    url = fs._generate_download_url(fs.PATH_CLS(test_url))
    assert url == expected_url


def test_generate_download_url_with_auth(dvc):
    config = {
        "url": VALID_NEXUS3_URL,
        "auth": "basic",
        "user": "benny",
        "password": "pass",
    }
    test_url = "nexus3://example.com/repository/repo/dvc_test/filename"
    expected_url = (
        "https://benny:pass@example.com/repository/repo/dvc_test/filename"
    )
    fs = Nexus3FileSystem(dvc, config)

    url = fs._generate_download_url(fs.PATH_CLS(test_url))
    assert url == expected_url

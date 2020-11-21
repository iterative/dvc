import os

import pytest
from mock import call, patch
from osfclient.models import File, Folder, OSFCore, Project
from osfclient.tests import mocks
from osfclient.tests.fake_responses import files_node, project_node

from dvc.path_info import URLInfo
from dvc.tree.osf import OSFTree

username = "example@mail.com"
data_dir = "data"
url = f"osf://odf.io/{data_dir}"
project = "abcd"
password = "12345"
config = {
    "url": url,
    "project": project,
    "osf_username": username,
    "password": password,
}


@pytest.fixture
def passwd_env_var():
    os.environ["OSF_PASSWORD"] = password
    yield

    del os.environ["OSF_PASSWORD"]


def test_init(dvc):
    tree = OSFTree(dvc, config)

    assert tree.path_info == url
    assert tree.project_guid == project
    assert tree.password == password
    assert tree.osf_username == username


def test_init_envvar(dvc, passwd_env_var):
    config_env = {"url": url, "project": project, "osf_username": username}
    tree = OSFTree(dvc, config_env)

    assert tree.password == password


@patch.object(
    OSFCore, "_get", return_value=mocks.FakeResponse(200, project_node)
)
def test_project(OSFCore_get, dvc):
    tree = OSFTree(dvc, config)
    proj = tree.project

    calls = [
        call("https://api.osf.io/v2//guids/abcd/"),
        call("https://api.osf.io/v2//nodes/abcd/"),
    ]
    OSFCore_get.assert_has_calls(calls)

    assert isinstance(proj, Project)


@patch.object(OSFCore, "_get")
def test_list_paths(OSFCore_get, dvc):
    _files_url = (
        f"https://api.osf.io/v2//nodes/{project}/files/osfstorage/foo123"
    )
    json = files_node(project, "osfstorage", ["foo/hello.txt", "foo/bye.txt"])
    response = mocks.FakeResponse(200, json)
    OSFCore_get.return_value = response

    store = Folder({})
    store._files_url = _files_url

    with patch.object(OSFTree, "storage", new=store):
        tree = OSFTree(dvc, config)
        files = list(tree._list_paths())
        assert len(files) == 2
        assert "/foo/hello.txt" in files
        assert "/foo/bye.txt" in files

    OSFCore_get.assert_called_once_with(_files_url)


@patch.object(OSFCore, "_get")
def test_get_file_obj(OSFCore_get, dvc):
    _files_url = (
        f"https://api.osf.io/v2//nodes/{project}/files/osfstorage/data123"
    )
    json = files_node(
        project, "osfstorage", ["data/hello.txt", "data/bye.txt"]
    )
    response = mocks.FakeResponse(200, json)
    OSFCore_get.return_value = response

    store = Folder({})
    store._files_url = _files_url

    path_info = URLInfo(url) / "hello.txt"

    with patch.object(OSFTree, "storage", new=store):
        tree = OSFTree(dvc, config)
        file = tree._get_file_obj(path_info)
        assert isinstance(file, File)
        assert file.path == "/data/hello.txt"

    OSFCore_get.assert_called_once_with(_files_url)


def test_is_dir(dvc):
    path_info = URLInfo(url) / "dir/"
    f = File({})

    f.path = "/data/dir/"
    with patch.object(OSFTree, "_get_file_obj", return_value=f):
        tree = OSFTree(dvc, config)
        assert tree.isdir(path_info)

    f.path = "/data/file"
    with patch.object(OSFTree, "_get_file_obj", return_value=f):
        tree = OSFTree(dvc, config)
        assert not tree.isdir(path_info)


def test_walk_files(dvc):
    path_info = URLInfo(url)

    f1 = "/data/dir/"
    f2 = "/data/file1"
    f3 = "/data/file2"

    with patch.object(OSFTree, "_list_paths", return_value=[f1, f2, f3]):
        tree = OSFTree(dvc, config)
        files = [i.url for i in tree.walk_files(path_info)]
        assert "osf://odf.io/data/file1" in files
        assert "osf://odf.io/data/file2" in files

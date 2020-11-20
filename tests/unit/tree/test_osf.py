import os

import pytest
from mock import call, patch
from osfclient.models import OSFCore, Project
from osfclient.tests import mocks
from osfclient.tests.fake_responses import project_node

from dvc.tree.osf import OSFTree

username = "example@mail.com"
data_dir = "data"
url = f"osf://odf.io/{data_dir}"
project = "abcd"
password = "12345"


@pytest.fixture
def passwd_env_var():
    os.environ["OSF_PASSWORD"] = password
    yield

    del os.environ["OSF_PASSWORD"]


def test_init(dvc):
    config = {
        "url": url,
        "project": project,
        "osf_username": username,
        "password": password,
    }
    tree = OSFTree(dvc, config)

    assert tree.path_info == url
    assert tree.project_guid == project
    assert tree.password == password
    assert tree.osf_username == username


def test_init_envvar(dvc, passwd_env_var):
    config = {"url": url, "project": project, "osf_username": username}
    tree = OSFTree(dvc, config)

    assert tree.password == password


@patch.object(
    OSFCore, "_get", return_value=mocks.FakeResponse(200, project_node)
)
def test_project(OSFCore_get, dvc):
    config = {
        "url": url,
        "project": project,
        "osf_username": username,
        "password": password,
    }
    tree = OSFTree(dvc, config)
    storage = tree.project

    calls = [
        call("https://api.osf.io/v2//guids/abcd/"),
        call("https://api.osf.io/v2//nodes/abcd/"),
    ]
    OSFCore_get.assert_has_calls(calls)

    assert isinstance(storage, Project)

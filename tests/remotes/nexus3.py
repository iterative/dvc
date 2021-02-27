# pylint:disable=abstract-method
import json
import os
import re

import pytest
import responses
from funcy import cached_property

from dvc.path_info import Nexus3UnsecureURLInfo

from ..basic_env import TestNexus3TempDir
from .base import Base

LOCAL_NEXUS3_HOSTNAME = "127.0.0.1:8081"
LOCAL_NEXUS3_USERNAME = "admin"
LOCAL_NEXUS3_PASSWORD = "6959d80e-932e-4e7f-8c66-7114d3020550"
LOCAL_NEXUS3_REPOSITORY = "dvc_test_repo"
LOCAL_NEXUS3_CLEANUP_SCRIPT_NAME = "cleanup_dvc_test_repo"


class RealLocalNexus3Backend(Base, Nexus3UnsecureURLInfo):
    @staticmethod
    def get_url():  # pylint: disable=arguments-differ
        return (
            f"nexus3://{LOCAL_NEXUS3_HOSTNAME}/repository/"
            f"{LOCAL_NEXUS3_REPOSITORY}/dvc_tests"
        )

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

    @cached_property
    def config(self):
        return {
            "url": self.get_url(),
            "auth": "basic",
            "user": LOCAL_NEXUS3_USERNAME,
            "password": LOCAL_NEXUS3_PASSWORD,
            "unsecure": True,  # HTTP backend for local testing
        }

    def write_bytes(self, contents):
        assert isinstance(contents, bytes)

        url_info = Nexus3UnsecureURLInfo(self.url)
        from dvc.fs import Nexus3FileSystem

        (
            _,
            repository,
            directory,
            filename,
        ) = Nexus3FileSystem.extract_nexus_repo_info_from_url(url_info)
        assert directory

        payload = {
            "raw.directory": (None, directory),
            "raw.asset1.filename": (None, filename),
            "raw.asset1": (filename, contents),
        }
        params = {"repository": repository}

        import requests
        from requests.auth import HTTPBasicAuth

        response = requests.post(
            f"http://{LOCAL_NEXUS3_HOSTNAME}/service/rest/v1/components"
            f"?repository={repository}",
            auth=HTTPBasicAuth(LOCAL_NEXUS3_USERNAME, LOCAL_NEXUS3_PASSWORD),
            files=payload,
            params=params,
        )
        assert response.status_code == 204


class Nexus3FileSystemMock(Base, Nexus3UnsecureURLInfo):
    nexus_file_storage: TestNexus3TempDir = None

    def set_mocked_storage(self, nexus_file_storage: TestNexus3TempDir):
        self.nexus_file_storage = nexus_file_storage

    @staticmethod
    def get_url():  # pylint: disable=arguments-differ
        return (
            f"nexus3://127.0.0.1:8081/repository/"
            f"{LOCAL_NEXUS3_REPOSITORY}/dvc_tests"
        )

    @cached_property
    def config(self):
        return {"url": self.get_url(), "unsecure": True}

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

    def write_bytes(self, contents):
        assert isinstance(contents, bytes)

        url_info = Nexus3UnsecureURLInfo(self.url)

        from dvc.fs import Nexus3FileSystem

        (
            _,
            repository,
            directory,
            filename,
        ) = Nexus3FileSystem.extract_nexus_repo_info_from_url(url_info)
        assert directory

        # streaming multipart 'form-data objects'
        # Nexus uses a 'form' for file submission
        from requests_toolbelt import MultipartEncoder

        multipart_encoder = MultipartEncoder(
            fields={
                "raw.directory": (None, directory),
                "raw.asset1.filename": (None, filename),
                "raw.asset1": (filename, contents),
            }
        )
        params = {"repository": repository}

        import requests
        from requests.auth import HTTPBasicAuth

        response = requests.post(
            f"http://127.0.0.1:8081/service/rest/v1/components?repo"
            f"sitory={repository}",
            auth=HTTPBasicAuth(LOCAL_NEXUS3_USERNAME, LOCAL_NEXUS3_PASSWORD),
            data=multipart_encoder,
            params=params,
        )
        assert response.status_code == 204


class Nexus3ClientMock:
    NEXUS_3_HEADER = {
        "Date": "Wed, 24 Feb 2021 22:27:50 GMT",
        "Server": "Nexus/3.25.0-03 (OSS)",
        "X-Content-Type-Options": "nosniff",
        "Content-Security-Policy": "sandbox allow-forms",
        "X-XSS-Protection": "1; mode=block",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate, "
        "post-check=0, pre-check=0",
        "Expires": "0",
        "X-Frame-Options": "DENY",
        "Content-Type": "text/html",
        "Content-Length": "1883",
    }
    NEXUS_COMPONENTS_URL = "/service/rest/v1/components"
    repository_api_path = f"/repository/{LOCAL_NEXUS3_REPOSITORY}/"
    nexus_file_storage: TestNexus3TempDir = None

    def __init__(self):
        self.nexus_file_storage = TestNexus3TempDir()

    def teardown(self):
        self.nexus_file_storage.tearDown()

    def _create_nexus_item_info_from_file(self, filename):
        file_info = self.nexus_file_storage.get_info_by_file_path(filename)
        return {
            "id": file_info["id"],
            "repository": LOCAL_NEXUS3_REPOSITORY,
            "group": f"/{os.path.dirname(filename)}",
            "name": filename,  # relative path without leading "/"
            "version": None,
            "assets": [
                {
                    "downloadUrl": f"http://127.0.0.1"
                    f":8"
                    f"081/repository/"
                    f"{LOCAL_NEXUS3_REPOSITORY}/{filename}",
                    "path": filename,
                    "id": file_info["id"],
                    "repository": LOCAL_NEXUS3_REPOSITORY,
                    "format": "raw",
                    "checksum": {
                        "sha1": "not_used",
                        "sha256": "not_used",
                        "sha512": "not_used",
                        "md5": file_info["md5"],
                    },
                }
            ],
        }

    def init_mocked_requests(self):
        """
        Register callback functions which intercept requests
        and return mocked Nexus 3 API responses
        """
        local_mocked_nexus_hostname = re.compile(
            "http://127\\.0\\.0\\.1:8081/.*"
        )
        with responses.RequestsMock(
            assert_all_requests_are_fired=False
        ) as rsps:
            rsps.add_callback(
                responses.HEAD,
                local_mocked_nexus_hostname,
                callback=self.fake_head_response,
            )
            rsps.add_callback(
                responses.GET,
                local_mocked_nexus_hostname,
                callback=self.fake_get_response,
            )
            rsps.add_callback(
                responses.POST,
                local_mocked_nexus_hostname,
                callback=self.fake_post_response,
            )
            rsps.add_callback(
                responses.DELETE,
                local_mocked_nexus_hostname,
                callback=self.fake_delete_response,
            )
            yield rsps

    def fake_head_response(self, request):
        """
        Fake a Nexus 3 request.HEAD response
        """
        file_path = request.path_url[len(self.repository_api_path) :]
        file_info = self.nexus_file_storage.get_info_by_file_path(file_path)
        if file_info:
            header_with_correct_size = self.NEXUS_3_HEADER
            header_with_correct_size["Content-Length"] = str(file_info["size"])
            return 200, header_with_correct_size, ""
        return 404, self.NEXUS_3_HEADER, ""

    def fake_get_response(self, request):
        """
        There are two types of GET requests used at the moment:
        To download files and to search by directory (ls)
        """
        # a search request
        # pylint: disable=no-else-return
        if request.path_url.startswith("/service/rest/v1/search"):
            # search_string can be f.e. "directory/directory/"
            # or "directory/directory*" if a recursive search is used
            search_string = request.params["group"]
            matches = []
            import fnmatch

            for (
                rel_file_path
            ) in self.nexus_file_storage.file_info_storage.keys():
                dirname = os.path.dirname(rel_file_path)
                if fnmatch.fnmatchcase(
                    dirname, search_string.lstrip("/")
                ):  # Nexus uses a "/" in front of directories
                    matches.append(
                        self._create_nexus_item_info_from_file(rel_file_path)
                    )
            return (
                200,
                self.NEXUS_3_HEADER,
                json.dumps({"items": matches, "continuationToken": None}),
            )

        # a file download request
        elif request.path_url.startswith(self.repository_api_path):
            file_path = (
                f"{self.nexus_file_storage.root_dir}/"
                f"{request.path_url[len(self.repository_api_path):]}"
            )
            if os.path.isfile(file_path):
                with open(file_path, "rb") as fobj:
                    header_with_correct_size = self.NEXUS_3_HEADER
                    header_with_correct_size["Content-Length"] = str(
                        os.path.getsize(file_path)
                    )
                    return 200, header_with_correct_size, fobj.read()
        return 404, self.NEXUS_3_HEADER, ""

    def fake_post_response(self, request):
        """
        Mock a upload file to Nexus3 response
        """
        if request.path_url.startswith(self.NEXUS_COMPONENTS_URL):
            full_file_path = (
                f"{request.body.fields['raw.directory'][1]}/"
                f"{request.body.fields['raw.asset1.filename'][1]}"
            )
            contents = request.body.fields["raw.asset1"][1]
            file_content = (
                contents if isinstance(contents, bytes) else contents.read()
            )
            self.nexus_file_storage.create(full_file_path, file_content)
            return 204, self.NEXUS_3_HEADER, ""
        return 404, self.NEXUS_3_HEADER, ""

    def fake_delete_response(self, request):
        """
        Mock a Nexus3 delete file response
        """
        if request.path_url.startswith(self.NEXUS_COMPONENTS_URL):
            file_id = request.path_url[len(self.NEXUS_COMPONENTS_URL) :]
            file_path = self.nexus_file_storage.get_file_path_by_id(file_id)
            if not file_path:  # no such file
                return 404, self.NEXUS_3_HEADER, ""
            self.nexus_file_storage.remove(file_path)
            return 204, self.NEXUS_3_HEADER, ""
        return 404, self.NEXUS_3_HEADER, ""


@pytest.fixture
def mocked_nexus3_client():
    nexus3_client_mock = Nexus3ClientMock()
    yield from nexus3_client_mock.init_mocked_requests()
    nexus3_client_mock.teardown()


@pytest.fixture(scope="function")
def nexus3(mocked_nexus3_client):
    url = Nexus3FileSystemMock.get_url()
    nexus_fs_mock = Nexus3FileSystemMock(url)
    yield nexus_fs_mock


@pytest.fixture(scope="function")
def real_nexus3():
    """
    If a local Nexus instance is available, you can replace the
    `nexus3` fixture above with this one to test against the real
    Nexus 3 instance and not mock the API.

    Create a RAW repsository on your local Nexus instance,
    named {LOCAL_NEXUS3_REPOSITORY}.

    If a real Nexus3 is used, a cleanup of the repository must be
    triggered after each test, therefore add the following script
    named {LOCAL_NEXUS3_CLEANUP_SCRIPT_NAME} to your local
    Nexus 3 installation.

    Please enable "scripting on your local Nexus3 instance and
    add the cleanup script to you local Nexus3 instance by
    executing the following API call:

        curl -X POST
        "http://{LOCAL_NEXUS3_HOSTNAME}/service/rest/v1/script"
        -H "accept: application/json"
        -H "Content-Type: application/json"
        -d "{ \"name\": \"{LOCAL_NEXUS3_CLEANUP_SCRIPT_NAME}\",
        \"content\": \"Paste from below, replace all newlines with '\n' \",
        \"type\": \"groovy\"}"

        import org.sonatype.nexus.repository.maintenance.MaintenanceService;
        repo = repository.getRepositoryManager().get(\"{ \
        LOCAL_NEXUS3_REPOSITORY}\")
        MaintenanceService maintenanceService = container.lookup( \
        MaintenanceService.class.name)
        maintenanceService.deleteFolder(repo, \"dvc_tests\")

    Beware, if you use a real Nexus3 instance, the
    `tests.func.test_fs.test_fs_ls_recursive` test may fail sometimes.
    Nexus3 uses an elasticsearch backend and the index is
    probably not updated fast enough. Adding a `sleep(1)`
    in the middle of the test, before fetching the files fixes the test.
    """
    url = RealLocalNexus3Backend.get_url()
    nexus_fs = RealLocalNexus3Backend(url)
    yield nexus_fs

    # cleanup the real local Nexus 3 instance
    import requests

    requests.post(
        f"http://{LOCAL_NEXUS3_USERNAME}:{LOCAL_NEXUS3_PASSWORD}@"
        f"{LOCAL_NEXUS3_HOSTNAME}/service/rest/v1/script/"
        f"{LOCAL_NEXUS3_CLEANUP_SCRIPT_NAME}/run",
        headers={"Content-type": "text/plain", "Accept": "application/json"},
    )

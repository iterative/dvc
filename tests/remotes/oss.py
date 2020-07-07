# pylint:disable=abstract-method
import uuid

import pytest

from dvc.path_info import CloudURLInfo

from .base import Base

TEST_OSS_REPO_BUCKET = "dvc-test"
TEST_OSS_ENDPOINT = "127.0.0.1:{port}"
TEST_OSS_ACCESS_KEY_ID = "AccessKeyID"
TEST_OSS_ACCESS_KEY_SECRET = "AccessKeySecret"


class OSS(Base, CloudURLInfo):
    pass


@pytest.fixture(scope="session")
def oss_server(docker_compose, docker_services):
    import oss2

    port = docker_services.port_for("oss", 8880)
    endpoint = TEST_OSS_ENDPOINT.format(port=port)

    def _check():
        try:
            auth = oss2.Auth(
                TEST_OSS_ACCESS_KEY_ID, TEST_OSS_ACCESS_KEY_SECRET
            )
            oss2.Bucket(auth, endpoint, "mybucket").get_bucket_info()
            return True
        except oss2.exceptions.NoSuchBucket:
            return True
        except oss2.exceptions.OssError:
            return False

    docker_services.wait_until_responsive(timeout=30.0, pause=5, check=_check)

    return endpoint


@pytest.fixture
def oss(oss_server):
    url = f"oss://{TEST_OSS_REPO_BUCKET}/{uuid.uuid4()}"
    ret = OSS(url)
    ret.config = {
        "url": url,
        "oss_key_id": TEST_OSS_ACCESS_KEY_ID,
        "oss_key_secret": TEST_OSS_ACCESS_KEY_SECRET,
        "oss_endpoint": oss_server,
    }
    return ret

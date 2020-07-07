import subprocess

import pytest

from .azure import Azure, azure, azure_server  # noqa: F401
from .hdfs import HDFS, hdfs  # noqa: F401
from .http import HTTP, http, http_server  # noqa: F401
from .local import Local, local_cloud, local_remote  # noqa: F401
from .oss import OSS, TEST_OSS_REPO_BUCKET, oss, oss_server  # noqa: F401
from .s3 import S3, TEST_AWS_REPO_BUCKET, real_s3, s3  # noqa: F401

TEST_REMOTE = "upstream"
TEST_CONFIG = {
    "cache": {},
    "core": {"remote": TEST_REMOTE},
    "remote": {TEST_REMOTE: {"url": ""}},
}


from .gdrive import (  # noqa: F401; noqa: F401
    TEST_GDRIVE_REPO_BUCKET,
    GDrive,
    gdrive,
)
from .gs import (  # noqa: F401; noqa: F401
    GCP,
    TEST_GCP_CREDS_FILE,
    TEST_GCP_REPO_BUCKET,
    gs,
)
from .ssh import (  # noqa: F401; noqa: F401
    SSH,
    SSHMocked,
    ssh,
    ssh_connection,
    ssh_server,
)


@pytest.fixture(scope="session")
def docker_compose():
    try:
        subprocess.check_output("docker-compose version", shell=True)
    except (subprocess.CalledProcessError, OSError):
        pytest.skip("no docker-compose installed")


@pytest.fixture
def remote(tmp_dir, dvc, request):
    cloud = request.param
    assert cloud
    tmp_dir.add_remote(config=cloud.config)
    yield cloud


@pytest.fixture
def workspace(tmp_dir, dvc, request):
    from dvc.cache import Cache

    cloud = request.param

    assert cloud

    tmp_dir.add_remote(name="workspace", config=cloud.config, default=False)
    tmp_dir.add_remote(
        name="cache", url="remote://workspace/cache", default=False
    )

    scheme = getattr(cloud, "scheme", "local")
    if scheme != "http":
        with dvc.config.edit() as conf:
            conf["cache"][scheme] = "cache"

        dvc.cache = Cache(dvc)

    return cloud

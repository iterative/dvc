import pytest

from .azure import Azure, azure, azure_remote  # noqa: F401
from .hdfs import HDFS, hdfs, hdfs_remote  # noqa: F401
from .http import HTTP, http, http_remote, http_server  # noqa: F401
from .local import Local, local_cloud, local_remote  # noqa: F401
from .oss import OSS, TEST_OSS_REPO_BUCKET, oss, oss_remote  # noqa: F401
from .s3 import (  # noqa: F401
    S3,
    TEST_AWS_REPO_BUCKET,
    real_s3,
    real_s3_remote,
    s3,
    s3_remote,
)

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
    gdrive_remote,
)
from .gs import (  # noqa: F401; noqa: F401
    GCP,
    TEST_GCP_CREDS_FILE,
    TEST_GCP_REPO_BUCKET,
    gs,
    gs_remote,
)
from .ssh import (  # noqa: F401; noqa: F401
    SSH,
    SSHMocked,
    ssh,
    ssh_connection,
    ssh_remote,
    ssh_server,
)


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

from .azure import Azure, azure, azure_server, make_azure  # noqa: F401
from .gdrive import (  # noqa: F401; noqa: F401
    TEST_GDRIVE_REPO_BUCKET,
    GDrive,
    gdrive,
    make_gdrive,
)
from .git_server import git_server, git_ssh  # noqa: F401
from .gs import (  # noqa: F401; noqa: F401
    GCP,
    TEST_GCP_CREDS_FILE,
    TEST_GCP_REPO_BUCKET,
    gs,
    make_gs,
)
from .hdfs import (  # noqa: F401
    HDFS,
    hadoop,
    hdfs,
    hdfs_server,
    make_hdfs,
    real_hdfs,
)
from .http import HTTP, http, http_server, make_http  # noqa: F401
from .local import Local, local_cloud, local_remote, make_local  # noqa: F401
from .oss import (  # noqa: F401
    OSS,
    TEST_OSS_REPO_BUCKET,
    make_oss,
    oss,
    oss_server,
    real_oss,
)
from .s3 import (  # noqa: F401
    S3,
    TEST_AWS_REPO_BUCKET,
    make_s3,
    real_s3,
    s3,
    s3_fake_creds_file,
    s3_server,
)
from .ssh import (  # noqa: F401; noqa: F401
    SSH,
    make_ssh,
    ssh,
    ssh_connection,
    ssh_server,
)
from .webdav import Webdav, make_webdav, webdav, webdav_server  # noqa: F401
from .webhdfs import WebHDFS, make_webhdfs, webhdfs  # noqa: F401

TEST_REMOTE = "upstream"
TEST_CONFIG = {
    "cache": {},
    "core": {"remote": TEST_REMOTE},
    "remote": {TEST_REMOTE: {"url": ""}},
}

# ruff: noqa: N999
from PyInstaller.utils.hooks import copy_metadata  # pylint:disable=import-error

# needed for `dvc doctor` to show dep versions
datas = copy_metadata("adlfs", recursive=True)
datas += copy_metadata("knack")
datas += copy_metadata("gcsfs")
datas += copy_metadata("pyarrow")
datas += copy_metadata("pydrive2")
datas += copy_metadata("s3fs", recursive=True)
datas += copy_metadata("boto3")
datas += copy_metadata("ossfs")
datas += copy_metadata("sshfs")
datas += copy_metadata("webdav4")
datas += copy_metadata("aiohttp")
datas += copy_metadata("aiohttp_retry")

hiddenimports = [
    "dvc_azure",
    "dvc_gdrive",
    "dvc_gs",
    "dvc_hdfs",
    "dvc_oss",
    "dvc_s3",
    "dvc_webdav",
    "dvc_webhdfs",
    # https://github.com/pypa/setuptools/issues/1963
    "pkg_resources.py2_warn",
]

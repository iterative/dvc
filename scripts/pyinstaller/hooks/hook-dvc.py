from PyInstaller.utils.hooks import (  # pylint:disable=import-error
    copy_metadata,
)

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

# https://github.com/pypa/setuptools/issues/1963
hiddenimports = ["pkg_resources.py2_warn"]

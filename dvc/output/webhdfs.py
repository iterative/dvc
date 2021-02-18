from dvc.output.base import BaseOutput

from ..fs.webhdfs import WebHDFSFileSystem


class WebHDFSOutput(BaseOutput):
    FS_CLS = WebHDFSFileSystem

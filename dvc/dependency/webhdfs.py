from dvc.dependency.base import BaseDependency
from dvc.output.webhdfs import WebHDFSOutput


class WebHDFSDependency(BaseDependency, WebHDFSOutput):
    pass

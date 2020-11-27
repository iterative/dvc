from dvc.output.base import BaseOutput

from ..tree.webhdfs import WebHDFSTree


class WebHDFSOutput(BaseOutput):
    TREE_CLS = WebHDFSTree

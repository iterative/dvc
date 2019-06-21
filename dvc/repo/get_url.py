import os

import dvc.output as output
import dvc.dependency as dependency

from dvc.utils.compat import urlparse


@staticmethod
def get_url(url, out=None):
    out = out or os.path.basename(urlparse(url).path)

    if os.path.exists(url):
        url = os.path.abspath(url)
        out = os.path.abspath(out)

    dep, = dependency.loads_from(None, [url])
    out, = output.loads_from(None, [out], use_cache=False)
    dep.download(out)

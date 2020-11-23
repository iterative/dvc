import os

import dvc.dependency as dependency
import dvc.output as output
from dvc.utils import resolve_output


def get_url(url, out=None):
    out = resolve_output(url, out)

    if os.path.exists(url):
        url = os.path.abspath(url)

    out = os.path.abspath(out)

    (dep,) = dependency.loads_from(None, [url])
    (out,) = output.loads_from(None, [out], use_cache=False)
    if not dep.exists:
        raise dep.DoesNotExistError(dep)
    dep.download(out)
    out.save()

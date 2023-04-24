import os

from dvc import output
from dvc.exceptions import URLMissingError
from dvc.fs import download, parse_external_url
from dvc.utils import resolve_output


def get_url(url, out=None, *, config=None, jobs=None, force=False):
    out = resolve_output(url, out, override=force, override_hint=True)
    out = os.path.abspath(out)
    (out,) = output.loads_from(None, [out], use_cache=False)

    src_fs, src_path = parse_external_url(url, config)
    if not src_fs.exists(src_path):
        raise URLMissingError(url)
    download(src_fs, src_path, out, jobs=jobs)

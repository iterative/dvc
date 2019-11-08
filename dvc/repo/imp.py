from dvc.config import NoRemoteError
from dvc.exceptions import RemoteNotSpecifiedInTargetRepoError


def imp(self, url, path, out=None, rev=None):
    erepo = {"url": url}
    if rev is not None:
        erepo["rev"] = rev

    try:
        return self.imp_url(path, out=out, erepo=erepo, locked=True)
    except NoRemoteError:
        raise RemoteNotSpecifiedInTargetRepoError(url)

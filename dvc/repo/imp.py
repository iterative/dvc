from dvc.exceptions import NoOutputInExternalRepoError
from dvc.exceptions import OutputNotFoundError


def imp(self, url, path, out=None, rev=None):
    erepo = {"url": url}
    if rev is not None:
        erepo["rev"] = rev

    try:
        return self.imp_url(path, out=out, erepo=erepo, locked=True)
    except OutputNotFoundError:
        raise NoOutputInExternalRepoError(url, path)

from dvc.external_repo import ExternalRepo


def imp(self, url, path, out=None, rev=None):
    erepo = {ExternalRepo.PARAM_URL: url}
    if rev is not None:
        erepo[ExternalRepo.PARAM_VERSION] = rev

    return self.imp_url(path, out=out, erepo=erepo)

def imp(self, url, path, out=None, rev=None, config=None, **kwargs):
    erepo = {"url": url}
    if rev is not None:
        erepo["rev"] = rev

    if config is not None:
        erepo["config"] = config

    return self.imp_url(path, out=out, erepo=erepo, frozen=True, **kwargs)

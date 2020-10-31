def imp(self, url, path, out=None, fname=None, rev=None, no_exec=False):
    erepo = {"url": url}
    if rev is not None:
        erepo["rev"] = rev

    return self.imp_url(
        path, out=out, fname=fname, erepo=erepo, frozen=True, no_exec=no_exec
    )

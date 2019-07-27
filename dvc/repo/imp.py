def imp(self, url, path, out=None, rev=None, remove_outs=False):
    erepo = {"url": url}
    if rev is not None:
        erepo["rev"] = rev

    return self.imp_url(
        path, out=out, erepo=erepo, locked=True, remove_outs=remove_outs
    )

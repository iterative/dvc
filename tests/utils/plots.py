import dpath

dpath.options.ALLOW_EMPTY_STRING_KEYS = True


def get_plot(plots_data, revision, typ="sources", file=None, endkey="data"):
    if file is not None:
        return dpath.get(plots_data, [revision, typ, "data", file, endkey])
    return dpath.get(plots_data, [revision, typ, endkey])

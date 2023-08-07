import os


def check_missing(repo, rev=None):
    from dvc_data.index import StorageKeyError

    with repo.switch(rev or "workspace"):
        index = repo.index.data["repo"]

    def onerror(_entry, _exc):
        pass

    index.onerror = onerror

    ret = []
    for _, entry in index.iteritems():
        try:
            fs, path = index.storage_map.get_cache(entry)
        except (StorageKeyError, ValueError):
            continue

        if not fs.exists(path):
            ret.append(os.path.join(*entry.key))

    return ret

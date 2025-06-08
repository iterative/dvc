import os


def check_missing(repo, rev=None, max_size=None, types=None):
    from dvc_data.index import StorageKeyError

    with repo.switch(rev or "workspace"):
        idx = repo.index.targets_view(None, max_size=max_size, types=types)

        index = idx.data["repo"]

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
            typ = "directory" if (entry.meta and entry.meta.isdir) else "file"
            ret.append(
                (
                    typ,
                    entry.hash_info.name,
                    entry.hash_info.value,
                    os.path.join(*entry.key),
                )
            )

    return ret

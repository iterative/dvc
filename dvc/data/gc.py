def gc(odb, used, jobs=None, cache_odb=None, shallow=True):
    from dvc.data.tree import Tree
    from dvc.objects.errors import ObjectDBPermissionError

    if odb.read_only:
        raise ObjectDBPermissionError("Cannot gc read-only ODB")
    if not cache_odb:
        cache_odb = odb
    used_hashes = set()
    for hash_info in used:
        used_hashes.add(hash_info.value)
        if hash_info.isdir and not shallow:
            tree = Tree.load(cache_odb, hash_info)
            used_hashes.update(
                entry_obj.hash_info.value for _, entry_obj in tree
            )

    def _is_dir_hash(_hash):
        from dvc.hash_info import HASH_DIR_SUFFIX

        return _hash.endswith(HASH_DIR_SUFFIX)

    removed = False
    # hashes must be sorted to ensure we always remove .dir files first
    for hash_ in sorted(
        odb.all(jobs, odb.fs_path),
        key=_is_dir_hash,
        reverse=True,
    ):
        if hash_ in used_hashes:
            continue
        fs_path = odb.hash_to_path(hash_)
        if _is_dir_hash(hash_):
            # backward compatibility
            # pylint: disable=protected-access
            odb._remove_unpacked_dir(hash_)
        odb.fs.remove(fs_path)
        removed = True

    return removed

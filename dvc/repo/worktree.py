from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dvc.cloud import Remote
    from dvc.repo import Repo


def fetch(repo: "Repo", remote: "Remote") -> int:
    from dvc_data.index import save

    index = repo.index.data["repo"]
    for key, entry in index.iteritems():
        entry.fs = remote.fs
        entry.path = remote.fs.path.join(
            remote.path,
            *key,
        )
    save(index)
    return len(index)


def push(repo: "Repo", remote: "Remote") -> int:
    from dvc_data.index import checkout
    from dvc_data.index.save import build_tree

    index = repo.index.data["repo"]
    checkout(index, remote.path, remote.fs)

    for stage in repo.index.stages:
        for out in stage.outs:
            if not out.use_cache:
                continue

            if not out.is_in_repo:
                continue

            workspace, key = out.index_key
            index = repo.index.data[workspace]
            entry = index[key]
            if out.isdir():
                old_tree = out.get_obj()
                entry.hash_info = old_tree.hash_info
                entry.meta = out.meta
                for subkey, entry in index.iteritems(key):
                    if entry.meta.isdir:
                        continue
                    fs_path = repo.fs.path.join(repo.root_dir, *subkey)
                    _, hash_info = old_tree.get(
                        repo.fs.path.relparts(fs_path, out.fs_path)
                    )
                    entry.hash_info = hash_info
                tree_meta, new_tree = build_tree(index, key)
                out.obj = new_tree
                out.hash_info = new_tree.hash_info
                out.meta = tree_meta
            else:
                out.hash_info = entry.hash_info
                out.meta = entry.meta
        stage.dvcfile.dump(stage, with_files=True, update_pipeline=False)

    return len(index)

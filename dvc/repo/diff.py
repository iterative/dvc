from __future__ import unicode_literals

import os


from dvc.scm.base import FileNotInCommitError


def _extract_dir(self, output):
    """Extract the content of dvc tree file
    Args:
        self(object) - Repo class instance
        output(object) - OutputLOCAL class instance
    Returns:
        dict - dictionary with keys - paths to file in .dvc/cache
                               values -checksums for that files
    """
    lst = output.dir_cache
    return {i["relpath"]: i["md5"] for i in lst}


def _get_tree_changes(self, a_entries, b_entries):
    result = {"del": 0, "ident": 0, "changes": 0, "new": 0, "moves": 0}
    keys = set(a_entries.keys())
    keys.update(b_entries.keys())
    diff_size = 0
    for key in keys:
        if key in a_entries and key in b_entries:
            if a_entries[key] == b_entries[key]:
                result["ident"] += 1
            else:
                result["changes"] += 1
                diff_size += os.path.getsize(
                    self.cache.local.get(b_entries[key])
                ) - os.path.getsize(self.cache.local.get(a_entries[key]))
        elif key in a_entries:
            result["del"] += 1
            diff_size -= os.path.getsize(self.cache.local.get(a_entries[key]))
        else:
            result["new"] += 1
            diff_size += os.path.getsize(self.cache.local.get(b_entries[key]))
    result["size_diff"] = diff_size
    return result


def _get_diff_outs(self, diff_dct):
    self.tree = diff_dct["a_tree"]
    a_outs = {str(out): out for st in self.stages() for out in st.outs}
    self.tree = diff_dct["b_tree"]
    b_outs = {str(out): out for st in self.stages() for out in st.outs}
    outs_paths = set(a_outs.keys())
    outs_paths.update(b_outs.keys())
    results = {}
    for path in outs_paths:
        if path in a_outs and path in b_outs:
            results[path] = {
                "a_output": a_outs[path],
                "b_output": b_outs[path],
                "new_file": False,
                "deleted_file": False,
                # possible drawback: regular file ->directory movement
                "is_dir": a_outs[path].is_dir_cache,
            }
        elif path in a_outs:
            results[path] = {
                "a_output": a_outs[path],
                "new_file": False,
                "deleted_file": True,
                "is_dir": a_outs[path].is_dir_cache,
            }
        else:
            results[path] = {
                "b_output": b_outs[path],
                "new_file": True,
                "deleted_file": False,
                "is_dir": b_outs[path].is_dir_cache,
            }
    return results


def _diff_dir(self, target, diff_dct):
    result = {"target": target}
    result["is_dir"] = True
    a_entries, b_entries = {}, {}
    if not diff_dct["new_file"]:
        result["old_file"] = target
        result["old_checksum"] = diff_dct["a_output"].checksum
        a_entries = _extract_dir(self, diff_dct["a_output"])
    if not diff_dct["deleted_file"]:
        result["new_file"] = target
        result["new_checksum"] = diff_dct["b_output"].checksum
        b_entries = _extract_dir(self, diff_dct["b_output"])
    result.update(_get_tree_changes(self, a_entries, b_entries))
    return result


def _diff_file(self, target, diff_dct):
    result = {"target": target}
    size = 0
    if not diff_dct["new_file"]:
        result["old_file"] = target
        result["old_checksum"] = diff_dct["a_output"].checksum
        size -= os.path.getsize(
            self.cache.local.get(diff_dct["a_output"].checksum)
        )
    print("first", size)
    if not diff_dct["deleted_file"]:
        result["new_file"] = target
        result["new_checksum"] = diff_dct["b_output"].checksum
        size += os.path.getsize(
            self.cache.local.get(diff_dct["b_output"].checksum)
        )
    print("second", size)
    result["size_diff"] = size
    return result


def _diff_royal(self, target, diff_dct):
    if diff_dct["is_dir"]:
        return _diff_dir(self, target, diff_dct)
    return _diff_file(self, target, diff_dct)


def diff(self, target, a_ref=None, b_ref=None):
    """Gerenates diff message string output

    Args:
        target(str) - file/directory to check diff of
        a_ref(str) - first git tag
        (optional) b_ref(str) - second git tag

    Returns:
        string: string of output message with diff info
    """
    result = {}
    diff_dct = self.scm.get_diff_trees(a_ref, b_ref=b_ref)
    result["a_ref"] = diff_dct["a_ref"]
    result["b_ref"] = diff_dct["b_ref"]
    if diff_dct["equal"]:
        result["equal"] = True
        return result
    result["diffs"] = []
    diff_outs = _get_diff_outs(self, diff_dct)
    if target is None:
        result["diffs"] = [
            _diff_royal(self, path, diff_outs[path]) for path in diff_outs
        ]
    elif target in diff_outs:
        result["diffs"] = [_diff_royal(self, target, diff_outs[target])]
    else:
        msg = "Have not found file/directory '{}' in the commits"
        raise FileNotInCommitError(msg.format(target))
    return result

from __future__ import unicode_literals

import os


from dvc.scm.base import FileNotInCommitError
import dvc.scm.base as git


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
    result = {
        git.DIFF_DEL: 0,
        git.DIFF_IDENT: 0,
        git.DIFF_CHANGE: 0,
        git.DIFF_NEW: 0,
        git.DIFF_MOVE: 0,
    }
    keys = set(a_entries.keys())
    keys.update(b_entries.keys())
    diff_size = 0
    for key in keys:
        if key in a_entries and key in b_entries:
            if a_entries[key] == b_entries[key]:
                result[git.DIFF_IDENT] += 1
            else:
                result[git.DIFF_CHANGE] += 1
                diff_size += os.path.getsize(
                    self.cache.local.get(b_entries[key])
                ) - os.path.getsize(self.cache.local.get(a_entries[key]))
        elif key in a_entries:
            result[git.DIFF_DEL] += 1
            diff_size -= os.path.getsize(self.cache.local.get(a_entries[key]))
        else:
            result[git.DIFF_NEW] += 1
            diff_size += os.path.getsize(self.cache.local.get(b_entries[key]))
    result[git.DIFF_SIZE] = diff_size
    return result


def _get_diff_outs(self, diff_dct):
    self.tree = diff_dct[git.DIFF_A_TREE]
    a_outs = {str(out): out for st in self.stages() for out in st.outs}
    self.tree = diff_dct[git.DIFF_B_TREE]
    b_outs = {str(out): out for st in self.stages() for out in st.outs}
    outs_paths = set(a_outs.keys())
    outs_paths.update(b_outs.keys())
    results = {}
    for path in outs_paths:
        if path in a_outs and path in b_outs:
            results[path] = {
                "a_output": a_outs[path],
                "b_output": b_outs[path],
                git.DIFF_NEW_FILE: False,
                "deleted_file": False,
                # possible drawback: regular file ->directory movement
                git.DIFF_IS_DIR: a_outs[path].is_dir_cache,
            }
        elif path in a_outs:
            results[path] = {
                "a_output": a_outs[path],
                git.DIFF_NEW_FILE: False,
                "deleted_file": True,
                git.DIFF_IS_DIR: a_outs[path].is_dir_cache,
            }
        else:
            results[path] = {
                "b_output": b_outs[path],
                git.DIFF_NEW_FILE: True,
                "deleted_file": False,
                git.DIFF_IS_DIR: b_outs[path].is_dir_cache,
            }
    return results


def _diff_dir(self, target, diff_dct):
    result = {git.DIFF_TARGET: target}
    result[git.DIFF_IS_DIR] = True
    a_entries, b_entries = {}, {}
    try:
        if not diff_dct[git.DIFF_NEW_FILE]:
            result[git.DIFF_OLD_FILE] = target
            result[git.DIFF_OLD_CHECKSUM] = diff_dct["a_output"].checksum
            a_entries = _extract_dir(self, diff_dct["a_output"])
        if not diff_dct["deleted_file"]:
            result[git.DIFF_NEW_FILE] = target
            result[git.DIFF_NEW_CHECKSUM] = diff_dct["b_output"].checksum
            b_entries = _extract_dir(self, diff_dct["b_output"])
        result.update(_get_tree_changes(self, a_entries, b_entries))
    except FileNotFoundError:
        result[git.DIFF_SIZE] = git.DIFF_SIZE_UNKNOWN
    return result


def _diff_file(self, target, diff_dct):
    result = {git.DIFF_TARGET: target}
    size = 0
    try:
        if not diff_dct[git.DIFF_NEW_FILE]:
            result[git.DIFF_OLD_FILE] = target
            result[git.DIFF_OLD_CHECKSUM] = diff_dct["a_output"].checksum
            size -= os.path.getsize(
                self.cache.local.get(diff_dct["a_output"].checksum)
            )
        if not diff_dct["deleted_file"]:
            result[git.DIFF_NEW_FILE] = target
            result[git.DIFF_NEW_CHECKSUM] = diff_dct["b_output"].checksum
            size += os.path.getsize(
                self.cache.local.get(diff_dct["b_output"].checksum)
            )
    except FileNotFoundError:
        size = git.DIFF_SIZE_UNKNOWN
    result[git.DIFF_SIZE] = size
    return result


def _diff_royal(self, target, diff_dct):
    if diff_dct[git.DIFF_IS_DIR]:
        return _diff_dir(self, target, diff_dct)
    return _diff_file(self, target, diff_dct)


def diff(self, a_ref, target=None, b_ref=None):
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
    result[git.DIFF_A_REF] = diff_dct[git.DIFF_A_REF]
    result[git.DIFF_B_REF] = diff_dct[git.DIFF_B_REF]
    if diff_dct[git.DIFF_EQUAL]:
        result[git.DIFF_EQUAL] = True
        return result
    result[git.DIFF_LIST] = []
    diff_outs = _get_diff_outs(self, diff_dct)
    if target is None:
        result[git.DIFF_LIST] = [
            _diff_royal(self, path, diff_outs[path]) for path in diff_outs
        ]
    elif target in diff_outs:
        result[git.DIFF_LIST] = [_diff_royal(self, target, diff_outs[target])]
    else:
        msg = "Have not found file/directory '{}' in the commits"
        raise FileNotInCommitError(msg.format(target))
    return result

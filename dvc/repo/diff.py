from __future__ import unicode_literals

import os
from errno import ENOENT


import dvc.logger as logger
from dvc.scm.base import FileNotInCommitError
from dvc.scm.git import (
    DIFF_A_TREE,
    DIFF_B_TREE,
    DIFF_A_REF,
    DIFF_B_REF,
    DIFF_EQUAL,
)


DIFF_TARGET = "target"
DIFF_IS_DIR = "is_dir"
DIFF_OLD_FILE = "old_file"
DIFF_OLD_CHECKSUM = "old_checksum"
DIFF_NEW_FILE = "new_file"
DIFF_NEW_CHECKSUM = "new_checksum"
DIFF_SIZE = "size_diff"
DIFF_DEL = "del"
DIFF_IDENT = "ident"
DIFF_CHANGE = "changes"
DIFF_NEW = "new"
DIFF_MOVE = "moves"
DIFF_LIST = "diffs"
DIFF_SIZE_UNKNOWN = "?"
DIFF_A_OUTPUT = "a_output"
DIFF_B_OUTPUT = "b_output"
DIFF_DELETED = "deleted_file"
DIFF_IS_NEW = "created_file"


def _file_not_exists(error, result):
    if error.errno == ENOENT:
        result.update({DIFF_SIZE: DIFF_SIZE_UNKNOWN})
    else:
        raise error


def _extract_dir(self, dir_not_exists, output):
    """Extract the content of dvc tree file
    Args:
        self(object) - Repo class instance
        dir_not_exists(bool) - flag for directory existence
        output(object) - OutputLOCAL class instance
    Returns:
        dict - dictionary with keys - paths to file in .dvc/cache
                               values -checksums for that files
    """
    if not dir_not_exists:
        lst = output.dir_cache
        return {i["relpath"]: i["md5"] for i in lst}
    return {}


def _get_dir_info(dir_not_exists, a_output):
    if not dir_not_exists:
        return str(a_output), a_output.checksum
    return "", ""


def _ident_files(a_entries, b_entries):
    keys = [
        key for key in a_entries.keys() if b_entries.get(key) == a_entries[key]
    ]
    return len(keys)


def _modified_files(self, a_entries, b_entries):
    keys = [key for key in a_entries.keys() if key in b_entries]
    diff_size = 0
    modified_count = 0
    for key in keys:
        if a_entries[key] != b_entries[key]:
            modified_count
            diff_size += os.path.getsize(
                self.cache.local.get(b_entries[key])
            ) - os.path.getsize(self.cache.local.get(a_entries[key]))
    return modified_count, diff_size


def _deleted_files(self, a_entries, b_entries):
    diff_size = 0
    deleted_count = 0
    for key, value in a_entries.items():
        if key not in b_entries:
            deleted_count += 1
            diff_size -= os.path.getsize(self.cache.local.get(a_entries[key]))
    return deleted_count, diff_size


def _new_files(self, a_entries, b_entries):
    diff_size = 0
    new_count = 0
    for key, value in b_entries.items():
        if key not in a_entries:
            new_count += 1
            diff_size += os.path.getsize(self.cache.local.get(b_entries[key]))
    return new_count, diff_size


def _get_tree_changes(self, a_entries, b_entries):
    result = {
        DIFF_DEL: 0,
        DIFF_IDENT: 0,
        DIFF_CHANGE: 0,
        DIFF_NEW: 0,
        DIFF_MOVE: 0,
    }
    result[DIFF_IDENT] = _ident_files(a_entries, b_entries)
    result[DIFF_CHANGE], diff_size = _modified_files(
        self, a_entries, b_entries
    )
    result[DIFF_SIZE] = diff_size
    result[DIFF_DEL], diff_size = _deleted_files(self, a_entries, b_entries)
    result[DIFF_SIZE] += diff_size
    result[DIFF_NEW], diff_size = _new_files(self, a_entries, b_entries)
    result[DIFF_SIZE] += diff_size
    return result


def _check_local_cache(a_out, is_checked):
    if a_out is not None and a_out.scheme != "local":
        is_checked.append(str(a_out))
        return True
    return False


def _is_dir(path, a_outs, b_outs):
    if a_outs.get(path):
        return a_outs[path].is_dir_checksum
    else:
        return b_outs[path].is_dir_checksum


def _get_diff_outs(self, diff_dct):
    self.tree = diff_dct[DIFF_A_TREE]
    a_outs = {str(out): out for st in self.stages() for out in st.outs}
    self.tree = diff_dct[DIFF_B_TREE]
    b_outs = {str(out): out for st in self.stages() for out in st.outs}
    outs_paths = set(a_outs.keys())
    outs_paths.update(b_outs.keys())
    results = {}
    non_local_cache = []
    for path in outs_paths:
        check1 = _check_local_cache(a_outs.get(path), non_local_cache)
        check2 = _check_local_cache(b_outs.get(path), non_local_cache)
        # skip files/directories with non-local cache for now
        if check1 or check2:
            continue
        results[path] = {}
        results[path][DIFF_A_OUTPUT] = a_outs.get(path)
        results[path][DIFF_B_OUTPUT] = b_outs.get(path)
        results[path][DIFF_IS_NEW] = path not in a_outs
        results[path][DIFF_DELETED] = path not in b_outs
        results[path][DIFF_IS_DIR] = _is_dir(path, a_outs, b_outs)
    if non_local_cache:
        logger.warning(
            "Diff is not supported for non-local outputs. Ignoring: {}".format(
                non_local_cache
            )
        )

    return results


def _diff_dir(self, target, diff_dct):
    result = {DIFF_TARGET: target}
    result[DIFF_IS_DIR] = True
    a_entries, b_entries = {}, {}
    try:
        a_entries = _extract_dir(
            self, diff_dct[DIFF_IS_NEW], diff_dct[DIFF_A_OUTPUT]
        )
        b_entries = _extract_dir(
            self, diff_dct[DIFF_DELETED], diff_dct[DIFF_B_OUTPUT]
        )
        result[DIFF_OLD_FILE], result[DIFF_OLD_CHECKSUM] = _get_dir_info(
            diff_dct[DIFF_IS_NEW], diff_dct[DIFF_A_OUTPUT]
        )
        result[DIFF_NEW_FILE], result[DIFF_NEW_CHECKSUM] = _get_dir_info(
            diff_dct[DIFF_DELETED], diff_dct[DIFF_B_OUTPUT]
        )
        result.update(_get_tree_changes(self, a_entries, b_entries))
    except IOError as e:
        _file_not_exists(e, result)
    return result


def _diff_file(self, target, diff_dct):
    result = {DIFF_TARGET: target}
    size = 0
    try:
        if not diff_dct[DIFF_IS_NEW]:
            result[DIFF_OLD_FILE] = target
            result[DIFF_OLD_CHECKSUM] = diff_dct[DIFF_A_OUTPUT].checksum
            size -= os.path.getsize(
                self.cache.local.get(diff_dct[DIFF_A_OUTPUT].checksum)
            )
        if not diff_dct[DIFF_DELETED]:
            result[DIFF_NEW_FILE] = target
            result[DIFF_NEW_CHECKSUM] = diff_dct[DIFF_B_OUTPUT].checksum
            size += os.path.getsize(
                self.cache.local.get(diff_dct[DIFF_B_OUTPUT].checksum)
            )
        result[DIFF_SIZE] = size
    except IOError as e:
        _file_not_exists(e, result)
    return result


def _diff_royal(self, target, diff_dct):
    if diff_dct[DIFF_IS_DIR]:
        return _diff_dir(self, target, diff_dct)
    return _diff_file(self, target, diff_dct)


def diff(self, a_ref, target=None, b_ref=None):
    """Gerenates diff message string output

    Args:
        target(str) - file/directory to check diff of
        a_ref(str) - first tag
        (optional) b_ref(str) - second git tag

    Returns:
        string: string of output message with diff info
    """
    result = {}
    diff_dct = self.scm.get_diff_trees(a_ref, b_ref=b_ref)
    result[DIFF_A_REF] = diff_dct[DIFF_A_REF]
    result[DIFF_B_REF] = diff_dct[DIFF_B_REF]
    if diff_dct[DIFF_EQUAL]:
        result[DIFF_EQUAL] = True
        return result
    result[DIFF_LIST] = []
    diff_outs = _get_diff_outs(self, diff_dct)
    if target is None:
        result[DIFF_LIST] = [
            _diff_royal(self, path, diff_outs[path]) for path in diff_outs
        ]
    elif target in diff_outs:
        result[DIFF_LIST] = [_diff_royal(self, target, diff_outs[target])]
    else:
        msg = "Have not found file/directory '{}' in the commits"
        raise FileNotInCommitError(msg.format(target))
    return result

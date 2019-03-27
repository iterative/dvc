from __future__ import unicode_literals

import os
import humanize
import inflect


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
    result["diff_size"] = humanize.naturalsize(abs(diff_size))
    result["diff_sign"] = "-" if diff_size < 0 else ""
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
    engine = inflect.engine()
    msg = ""
    a_entries, b_entries = {}, {}
    if not diff_dct["new_file"]:
        msg += "-{} with md5 {}\n".format(
            target, diff_dct["a_output"].checksum
        )
        a_entries = _extract_dir(self, diff_dct["a_output"])
    if not diff_dct["deleted_file"]:
        msg += "+{} with md5 {}\n".format(
            target, diff_dct["b_output"].checksum
        )
        b_entries = _extract_dir(self, diff_dct["b_output"])
    msg += "\n"
    result = _get_tree_changes(self, a_entries, b_entries)
    msg += f"{result['ident']} " + engine.plural("file", result["ident"])
    msg += " not changed, "
    msg += f"{result['changes']} " + engine.plural("file", result["changes"])
    msg += " modified, "
    msg += f"{result['new']} " + engine.plural("file", result["new"])
    msg += " added, "
    msg += f"{result['del']} " + engine.plural("file", result["del"])
    msg += " deleted, "
    if result["diff_size"] == "0 Bytes":
        msg += "size wasn't changed"
    else:
        msg += "size was " + "increased" * (result["diff_sign"] != "-")
        msg += (
            "decreased" * (result["diff_sign"] == "-")
            + " by "
            + result["diff_size"]
        )
    return msg


def _diff_file(self, target, diff_dct):
    msg = ""
    size = 0
    if not diff_dct["new_file"]:
        msg += "-{} with md5 {}\n".format(
            target, diff_dct["a_output"].checksum
        )
        size -= os.path.getsize(
            self.cache.local.get(diff_dct["a_output"].checksum)
        )
    if not diff_dct["deleted_file"]:
        msg += "+{} with md5 {}\n\n".format(
            target, diff_dct["b_output"].checksum
        )
        size += os.path.getsize(
            self.cache.local.get(diff_dct["b_output"].checksum)
        )
    if not diff_dct["new_file"] and not diff_dct["deleted_file"] and size == 0:
        msg += "file was not changed"
    elif diff_dct["new_file"]:
        msg += "added file with size {}".format(humanize.naturalsize(size))
    elif diff_dct["deleted_file"]:
        msg += "deleted file with size {}".format(humanize.naturalsize(size))
    else:
        msg += "file was modified, file size " + "increased" * (size >= 0)
        msg += "decreased" * (size < 0) + " by " + humanize.naturalsize(size)
    return msg


def _diff_royal(self, target, diff_dct):
    msg = "diff for '" + target + "'\n"
    if diff_dct["is_dir"]:
        return msg + _diff_dir(self, target, diff_dct)
    return msg + _diff_file(self, target, diff_dct)


def diff(self, target, a_ref=None, b_ref=None):
    """Gerenates diff message string output

    Args:
        target(str) - file/directory to check diff of
        a_ref(str) - first git tag
        (optional) b_ref(str) - second git tag

    Returns:
        string: string of output message with diff info
    """
    diff_dct = self.scm.get_diff_trees(a_ref, b_ref=b_ref)
    msg = "dvc diff from {} to {}\n\n".format(
        diff_dct["a_ref"], diff_dct["b_ref"]
    )
    if diff_dct["equal"]:
        return msg
    diff_outs = _get_diff_outs(self, diff_dct)
    if target is None:
        for path in diff_outs:
            info_msg = _diff_royal(self, path, diff_outs[path])
            msg += info_msg
            msg += "\n\n"
        msg = msg[:-2]
    elif target in diff_outs:
        info_msg = _diff_royal(self, target, diff_outs[target])
        msg += info_msg
    else:
        msg = "Have not found file/directory '{}' in the commits"
        raise FileNotInCommitError(msg.format(target))
    return msg

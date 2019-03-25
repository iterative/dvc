from __future__ import unicode_literals

import os
import humanize
import inflect


def _extract_tree(self, cache_dir):
    lst = self.cache.local.load_dir_cache(cache_dir)
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


def _diff_dir(self, target, diff_dct):
    engine = inflect.engine()
    msg = ""
    a_entries, b_entries = {}, {}
    if not diff_dct["new_file"]:
        msg += "-{} with md5 {}\n".format(
            target, diff_dct["a_fname"].split(".dir")[0]
        )
        a_entries = _extract_tree(self, diff_dct["a_fname"])
    if not diff_dct["deleted_file"]:
        msg += "+{} with md5 {}\n".format(
            target, diff_dct["b_fname"].split(".dir")[0]
        )
        b_entries = _extract_tree(self, diff_dct["b_fname"])
    msg += "\n"
    result = _get_tree_changes(self, a_entries, b_entries)
    msg += f"{result['ident']} " + engine.plural("file", result["ident"])
    msg += " didn't change, "
    msg += f"{result['changes']} " + engine.plural("file", result["changes"])
    msg += " modified, "
    msg += f"{result['new']} " + engine.plural("file", result["new"])
    msg += " added, "
    msg += f"{result['del']} " + engine.plural("file", result["del"])
    msg += " deleted, "
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
            target, diff_dct["a_fname"].split(".dir")[0]
        )
        size -= os.path.getsize(self.cache.local.get(diff_dct["a_fname"]))
    if not diff_dct["deleted_file"]:
        msg += "+{} with md5 {}\n".format(
            target, diff_dct["b_fname"].split(".dir")[0]
        )
        size += os.path.getsize(self.cache.local.get(diff_dct["b_fname"]))
    if not diff_dct["new_file"] and not diff_dct["deleted_file"] and size == 0:
        msg += "file did not change\n"
    elif diff_dct["new_file"]:
        msg += "added file with size {}".format(humanize.naturalsize(size))
    elif diff_dct["deleted_file"]:
        msg += "deleted file with size {}".format(humanize.naturalsize(size))
    else:
        msg += "file was modified, file size " + "increased" * (size >= 0)
        msg += "decreased" * (size < 0) + " by " + humanize.naturalsize(size)
    return msg


def diff(self, target, a_ref=None, b_ref=None):
    """
    Gerenates diff message string output
    input:
    target - file/folder to check diff of
    a_ref - first git ref/tag
    b_ref(optional) - second git ref/tag
    output:
    string of output message with diff info
    """
    diff_dct = self.scm.diff(target, a_ref=a_ref, b_ref=b_ref)
    msg = "dvc diff for '{}' from {} to {} \n\n".format(
        target, diff_dct["a_tag"], diff_dct["b_tag"]
    )
    if diff_dct["equal"]:
        return msg
    if diff_dct["is_dir"]:
        dir_info = _diff_dir(self, target, diff_dct)
        msg += dir_info
    else:
        file_info = _diff_file(self, target, diff_dct)
        msg += file_info
    return msg

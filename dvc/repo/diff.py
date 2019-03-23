import os
import math

import dvc.logger as logger


def _extract_tree(self, cache_dir):
    lst = self.cache.local.load_dir_cache(cache_dir)
    return {i["relpath"]: i["md5"] for i in lst}


def _convert_size(size_bytes):
    sign = "" if size_bytes > 0 else "-"
    if size_bytes > 0:
        sign = ""
    else:
        size_bytes = -size_bytes
        sign = "-"
    size_bytes = abs(size_bytes)
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i]), sign


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
    result["diff_size"], result["diff_sign"] = _convert_size(diff_size)
    return result


def plural(s):
    return str(s) + " file" + "s" * (s > 1)


def _diff_dir(self, target, diff_dct):
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
    msg += plural(result["ident"]) + " didn't change, "
    msg += plural(result["changes"]) + " modified, "
    msg += plural(result["new"]) + " added, "
    msg += plural(result["del"]) + " deleted, "
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
    # os.path.getsize(self.cache.local.get(diff_obj['a_fname']))
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
        msg += "added file with size %s" % _convert_size(size)[0]
    elif diff_dct["deleted_file"]:
        msg += "deleted file with size %s" % _convert_size(size)[0]
    else:
        msg += "file was modified, file size " + "increased" * (size >= 0)
        msg += "decreased" * (size < 0) + " by " + _convert_size(size)[0]
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
        logger.info(msg)
        return msg
    if diff_dct["is_dir"]:
        dir_info = _diff_dir(self, target, diff_dct)
        msg += dir_info
    else:
        file_info = _diff_file(self, target, diff_dct)
        msg += file_info
    logger.info(msg)
    return msg

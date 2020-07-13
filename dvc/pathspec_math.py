# Path Specification Pattern Math
# Including changing base dir of path specification patterns and merging
# of two path specification patterns with different base
# All the operations follow the documents of `gitignore`
import os

from pathspec.util import normalize_file


def _include_rule(rule):
    return (True, rule[1:]) if rule.startswith("!") else (False, rule)


def _is_comment(rule):
    return rule.startswith("#")


def _remove_slash(rule):
    if rule.startswith("\\"):
        return rule[1:]
    return rule


def _match_all_level(rule):
    if rule[:-1].find("/") >= 0 and not rule.startswith("**/"):
        if rule.startswith("/"):
            rule = rule[1:]
        return False, rule
    if rule.startswith("**/"):
        rule = rule[3:]
    return True, rule


def change_rule(rule, rel):
    rule = rule.strip()
    if _is_comment(rule):
        return rule
    is_include, rule = _include_rule(rule)
    match_all, rule = _match_all_level(rule)
    rule = _remove_slash(rule)
    if not match_all:
        rule = f"/{rule}"
    else:
        rule = f"/**/{rule}"
    if is_include:
        rule = f"!/{rel}{rule}"
    else:
        rule = f"/{rel}{rule}"
    rule = normalize_file(rule)
    return rule


def _change_dirname(dirname, pattern_list, new_dirname):
    if new_dirname == dirname:
        return pattern_list
    rel = os.path.relpath(dirname, new_dirname)
    if rel.startswith(".."):
        raise ValueError("change dirname can only change to parent path")

    return [change_rule(rule, rel) for rule in pattern_list]


def _longest_common_dir(dir1, dir2):
    dir1_split = dir1.split(os.sep)
    dir2_split = dir2.split(os.sep)
    max_match = 0

    for index, (i, j) in enumerate(zip(dir1_split, dir2_split)):
        if i != j:
            break
        max_match = index
    return os.sep.join(dir1_split[: max_match + 1])


def merge_patterns(prefix_a, pattern_a, prefix_b, pattern_b):
    """
    Merge two path specification patterns.

    This implementation merge two path specification patterns on different
    bases. It returns the longest common parent directory, and the patterns
    based on this new base directory.
    """
    if not pattern_a:
        return prefix_b, pattern_b
    elif not pattern_b:
        return prefix_a, pattern_a

    longest_common_dir = _longest_common_dir(prefix_a, prefix_b)
    new_pattern_a = _change_dirname(prefix_a, pattern_a, longest_common_dir)
    new_pattern_b = _change_dirname(prefix_b, pattern_b, longest_common_dir)

    if len(prefix_a) < len(prefix_b):
        merged_pattern = new_pattern_a + new_pattern_b
    else:
        merged_pattern = new_pattern_b + new_pattern_a

    return longest_common_dir, merged_pattern

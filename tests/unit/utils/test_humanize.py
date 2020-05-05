from collections import OrderedDict

from dvc.utils.humanize import get_summary


def test_get_summary():
    # dict, so that we could delete from it easily
    stats = OrderedDict(
        [
            ("fetched", 3),
            ("added", ["file1", "file2", "file3"]),
            ("deleted", ["file4", "file5"]),
            ("modified", ["file6", "file7"]),
        ]
    )

    assert get_summary(stats.items()) == (
        "3 files fetched, "
        "3 files added, "
        "2 files deleted "
        "and "
        "2 files modified"
    )

    del stats["fetched"]
    del stats["deleted"][1]
    assert (
        get_summary(stats.items())
        == "3 files added, 1 file deleted and 2 files modified"
    )

    del stats["deleted"][0]
    assert get_summary(stats.items()) == "3 files added and 2 files modified"

    del stats["modified"]
    assert get_summary(stats.items()) == "3 files added"

    assert get_summary([]) == ""
    assert get_summary([("x", 0), ("y", [])]) == ""
    assert get_summary([("x", 1), ("y", [])]) == "1 file x"

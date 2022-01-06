import pytest

from dvc.exceptions import DvcException

DVC_FILE_SUFFIX = ".dvc"
META_KEY = "meta"
NOTES_KEY = "notes"


def check_notes_are(folder, subject, expected):
    if type(subject) is list:
        return all(check_notes_are(folder, s, expected) for s in subject)

    assert type(subject) is str
    if not subject.endswith(DVC_FILE_SUFFIX):
        subject += DVC_FILE_SUFFIX
    data = (folder / subject).parse()

    if META_KEY not in data:
        return expected is None
    if NOTES_KEY not in data[META_KEY]:
        return expected is None
    return data[META_KEY][NOTES_KEY] == expected


def note_setup(tmp_dir):
    tmp_dir.scm_gen({"README.md": "readme"}, commit="init git")
    tmp_dir.dvc_gen(
        {
            "alpha.txt": "content",
            "beta.txt": "content",
            "gamma/delta.txt": "content",
        },
        commit="init dvc",
    )


def test_note_fails_for_unknown_verb(dvc):
    with pytest.raises(DvcException):
        dvc.note("what", ["alpha.txt"], "color", "green")


def test_note_no_keys_initially_present(tmp_dir, scm, dvc):
    note_setup(tmp_dir)
    assert check_notes_are(tmp_dir, "alpha.txt", None)


def test_note_set_fails_key_not_given(dvc):
    with pytest.raises(DvcException):
        dvc.note("set", ["alpha.txt"], None, "green")


def test_note_set_fails_value_not_given(dvc):
    with pytest.raises(DvcException):
        dvc.note("set", ["alpha.txt"], "color", None)


def test_note_set_one_key_in_one_file(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    dvc.note("set", ["alpha.txt"], "color", "green")

    assert check_notes_are(tmp_dir, "alpha.txt", {"color": "green"})


def test_note_set_one_key_in_multiple_files(tmp_dir, scm, dvc):
    note_setup(tmp_dir)
    paths = ["alpha.txt", "beta.txt", "gamma/delta.txt"]

    dvc.note("set", paths, "color", "green")

    assert all(check_notes_are(tmp_dir, f, {"color": "green"}) for f in paths)


def test_note_set_another_key_in_one_file(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    dvc.note("set", ["alpha.txt"], "color", "green")
    dvc.note("set", ["alpha.txt"], "size", "large")

    check_notes_are(tmp_dir, "alpha.txt", {"color": "green", "size": "large"})


def test_note_set_fails_file_or_dvc_doesnt_exist(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    with pytest.raises(DvcException):
        dvc.note("set", ["README.md"], "color", "green")

    with pytest.raises(DvcException):
        dvc.note("set", ["nonexistent.txt"], "color", "green")


def test_note_set_fails_no_change_any_file_missing(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    with pytest.raises(DvcException):
        dvc.note("set", ["alpha.txt", "README.md"], "color", "green")
    check_notes_are(tmp_dir, "alpha.txt", None)

    with pytest.raises(DvcException):
        dvc.note("set", ["alpha.txt", "nonexistent.txt"], "color", "green")
    check_notes_are(tmp_dir, "alpha.txt", None)

    dvc.note("set", ["alpha.txt"], "color", "green")
    with pytest.raises(DvcException):
        dvc.note("set", ["alpha.txt", "README.md"], "color", "blue")
    check_notes_are(tmp_dir, "alpha.txt", {"color": "green"})

    with pytest.raises(DvcException):
        dvc.note("set", ["alpha.txt", "nonexistent.txt"], "color", "blue")
    check_notes_are(tmp_dir, "alpha.txt", {"color": "green"})


def test_note_find_none_none_added(tmp_dir, scm, dvc):
    note_setup(tmp_dir)
    assert dvc.note("find", ["alpha.txt"], "color") == []


def test_note_find_one_one_added(tmp_dir, scm, dvc):
    note_setup(tmp_dir)
    dvc.note("set", ["alpha.txt"], "color", "green")
    actual = dvc.note("find", ["alpha.txt"], "color")
    assert actual == [["alpha.txt", "color", "green"]]


def test_note_find_one_many_added_to_one_file(tmp_dir, scm, dvc):
    note_setup(tmp_dir)
    dvc.note("set", ["alpha.txt"], "color", "green")
    dvc.note("set", ["alpha.txt"], "size", "medium")
    actual = dvc.note("find", ["alpha.txt"], "color")
    assert actual == [["alpha.txt", "color", "green"]]


def test_note_find_none_looking_for_another_key(tmp_dir, scm, dvc):
    note_setup(tmp_dir)
    dvc.note("set", ["alpha.txt"], "color", "green")
    assert dvc.note("find", ["alpha.txt"], "size") == []


def test_note_find_some_multiple_keys_in_multiple_files(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    dvc.note("set", ["alpha.txt"], "color", "green")
    dvc.note("set", ["beta.txt"], "color", "green")
    dvc.note("set", ["beta.txt"], "size", "small")
    dvc.note("set", ["gamma/delta.txt"], "size", "medium")

    paths = ["alpha.txt", "beta.txt", "gamma/delta.txt"]
    assert dvc.note("find", paths, "size") == [
        ["beta.txt", "size", "small"],
        ["gamma/delta.txt", "size", "medium"],
    ]


def test_note_find_fails_file_doesnt_exist(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    with pytest.raises(DvcException):
        dvc.note("find", ["alpha.txt", "README.md"], "color", "green")

    with pytest.raises(DvcException):
        dvc.note("set", ["alpha.txt", "nonexistent.txt"], "color", "green")


def test_note_list_fails_file_doesnt_exist(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    with pytest.raises(DvcException):
        dvc.note("list", ["alpha.txt", "README.md"], "color", "green")

    with pytest.raises(DvcException):
        dvc.note("list", ["alpha.txt", "nonexistent.txt"], "color", "green")


def test_note_list_one_added(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    dvc.note("set", ["alpha.txt"], "color", "green")

    assert dvc.note("list", ["alpha.txt"]) == [["alpha.txt", ["color"]]]


def test_note_list_many_added_to_one_file(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    dvc.note("set", ["alpha.txt"], "color", "green")
    dvc.note("set", ["alpha.txt"], "size", "medium")

    actual = dvc.note("list", ["alpha.txt"])
    assert actual == [["alpha.txt", ["color", "size"]]]


def test_note_list_many_added_to_many_files(tmp_dir, scm, dvc):
    note_setup(tmp_dir)
    paths = ["alpha.txt", "beta.txt", "gamma/delta.txt"]

    dvc.note("set", ["alpha.txt"], "color", "green")
    dvc.note("set", ["alpha.txt"], "size", "medium")
    dvc.note("set", ["beta.txt"], "color", "blue")
    dvc.note("set", ["gamma/delta.txt"], "flavor", "strawberry")

    assert dvc.note("list", paths) == [
        ["alpha.txt", ["color", "size"]],
        ["beta.txt", ["color"]],
        ["gamma/delta.txt", ["flavor"]],
    ]


def test_note_remove_fails_key_not_given(dvc):
    with pytest.raises(DvcException):
        dvc.note("remove", ["alpha.txt"], None, "green")


def test_note_remove_fails_file_doesnt_exist(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    dvc.note("set", ["alpha.txt"], "color", "green")
    with pytest.raises(DvcException):
        dvc.note("remove", ["alpha.txt", "README.md"], "color")
    assert check_notes_are(tmp_dir, "alpha.txt", {"color": "green"})

    with pytest.raises(DvcException):
        dvc.note("remove", ["alpha.txt", "nonexistent.txt"], "color")
    assert check_notes_are(tmp_dir, "alpha.txt", {"color": "green"})


def test_note_remove_one_from_one_file(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    dvc.note("set", ["alpha.txt"], "color", "green")
    dvc.note("remove", ["alpha.txt"], "color")

    assert check_notes_are(tmp_dir, "alpha.txt", None)


def test_note_remove_one_from_multiple_files(tmp_dir, scm, dvc):
    note_setup(tmp_dir)
    paths = ["alpha.txt", "beta.txt", "gamma/delta.txt"]

    dvc.note("set", paths, "color", "green")
    dvc.note("remove", paths, "color")

    assert all(check_notes_are(tmp_dir, f, None) for f in paths)


def test_note_remove_selected_key_from_single_file(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    dvc.note("set", ["alpha.txt"], "color", "green")
    dvc.note("set", ["alpha.txt"], "size", "large")
    dvc.note("remove", ["alpha.txt"], "size")

    check_notes_are(tmp_dir, "alpha.txt", {"color": "green"})


def test_note_remove_selected_key_from_many_files(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    dvc.note("set", ["alpha.txt"], "color", "green")
    dvc.note("set", ["alpha.txt"], "size", "medium")
    dvc.note("set", ["beta.txt"], "color", "blue")
    dvc.note("set", ["beta.txt"], "size", "large")
    dvc.note("remove", ["alpha.txt", "beta.txt"], "color")

    check_notes_are(tmp_dir, "alpha.txt", {"size": "medium"})
    check_notes_are(tmp_dir, "beta.txt", {"size": "large"})


def test_note_remove_key_from_some_files(tmp_dir, scm, dvc):
    note_setup(tmp_dir)

    dvc.note("set", ["alpha.txt"], "color", "green")
    dvc.note("set", ["beta.txt"], "color", "blue")
    dvc.note("set", ["beta.txt"], "size", "large")
    dvc.note("set", ["gamma/delta.txt"], "size", "medium")
    dvc.note("remove", ["alpha.txt", "beta.txt", "gamma/delta.txt"], "color")

    check_notes_are(tmp_dir, "alpha.txt", None)
    check_notes_are(tmp_dir, "beta.txt", {"size": "large"})
    check_notes_are(tmp_dir, "gamma/delta.txt", {"size": "medium"})

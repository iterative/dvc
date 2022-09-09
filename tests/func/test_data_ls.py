from operator import itemgetter

from dvc.repo.data import ls


def test_data_ls(tmp_dir, dvc):
    assert not list(ls(dvc))

    tmp_dir.dvc_gen("bar", "bar")
    tmp_dir.gen("foo", "foo")
    dvc.add(
        "foo",
        meta={"key": "value"},
        labels=["l1", "l2"],
        type="t1",
        desc="foo",
    )

    foo_entry = {
        "path": "foo",
        "desc": "foo",
        "type": "t1",
        "labels": ["l1", "l2"],
        "meta": {"key": "value"},
    }
    assert sorted(ls(dvc), key=itemgetter("path")) == [
        {"path": "bar"},
        foo_entry,
    ]
    assert list(ls(dvc, targets=["foo"])) == [foo_entry]
    assert list(ls(dvc, targets=["foo"], recursive=True)) == [foo_entry]

import os

import mock

from dvc.hash_info import HashInfo
from dvc.path_info import PathInfo
from dvc.state import State
from dvc.utils import file_md5


def test_state(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo content")
    path = tmp_dir / "foo"
    path_info = PathInfo(path)
    hash_info = HashInfo("md5", file_md5(path, dvc.fs))

    state = State(dvc)

    with state:
        state.save(path_info, dvc.fs, hash_info)
        assert state.get(path_info, dvc.fs) == hash_info

        path.unlink()
        path.write_text("1")

        assert state.get(path_info, dvc.fs) is None

        hash_info = HashInfo("md5", file_md5(path, dvc.fs))
        state.save(path_info, dvc.fs, hash_info)

        assert state.get(path_info, dvc.fs) == hash_info


def test_state_overflow(tmp_dir, dvc):
    # NOTE: trying to add more entries than state can handle,
    # to see if it will clean up and vacuum successfully
    dvc.config["state"]["row_limit"] = 10

    path = tmp_dir / "dir"
    path.mkdir()
    for i in range(20):
        (path / str(i)).write_text(str(i))

    dvc.add("dir")


def mock_get_inode(inode):
    def get_inode_mocked(_):
        return inode

    return get_inode_mocked


@mock.patch("dvc.state.get_inode", autospec=True)
def test_get_state_record_for_inode(get_inode_mock, tmp_dir, dvc):
    tmp_dir.gen("foo", "foo content")

    state = State(dvc)
    inode = state.MAX_INT + 2
    assert inode != state._to_sqlite(inode)

    foo = tmp_dir / "foo"
    md5 = file_md5(foo, dvc.fs)
    get_inode_mock.side_effect = mock_get_inode(inode)

    with state:
        state.save(PathInfo(foo), dvc.fs, HashInfo("md5", md5))
        ret = state.get_state_record_for_inode(inode)
        assert ret is not None


def test_remove_links(tmp_dir, dvc):
    tmp_dir.dvc_gen({"foo": "foo_content", "bar": "bar_content"})

    with dvc.state:
        cmd_count_links = "SELECT count(*) FROM {}".format(
            State.LINK_STATE_TABLE
        )
        result = dvc.state._execute(cmd_count_links).fetchone()[0]
        assert result == 2

        dvc.state.remove_links(["foo", "bar"], dvc.fs)

        result = dvc.state._execute(cmd_count_links).fetchone()[0]
        assert result == 0


def test_get_unused_links(tmp_dir, dvc):
    tmp_dir.dvc_gen({"foo": "foo_content", "bar": "bar_content"})

    with dvc.state:
        links = [os.path.join(dvc.root_dir, link) for link in ["foo", "bar"]]
        assert set(dvc.state.get_unused_links([], dvc.fs)) == {"foo", "bar"}
        assert set(dvc.state.get_unused_links(links[:1], dvc.fs)) == {"bar"}
        assert set(dvc.state.get_unused_links(links, dvc.fs)) == set()
        assert set(
            dvc.state.get_unused_links(
                (
                    links[:1]
                    + [os.path.join(dvc.root_dir, "not-existing-file")]
                ),
                dvc.fs,
            )
        ) == {"bar"}

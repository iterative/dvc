import mock

from dvc.path_info import PathInfo
from dvc.state import State
from dvc.utils import file_md5


def test_state(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo content")
    path = tmp_dir / "foo"
    path_info = PathInfo(path)
    md5 = file_md5(path)[0]

    state = State(dvc, dvc.config.config)

    with state:
        state.save(path_info, md5)
        entry_md5 = state.get(path_info)
        assert entry_md5 == md5

        path.unlink()
        path.write_text("1")

        entry_md5 = state.get(path_info)
        assert entry_md5 is None

        md5 = file_md5(path)[0]
        state.save(path_info, md5)

        entry_md5 = state.get(path_info)
        assert entry_md5 == md5


def test_state_overflow(tmp_dir, dvc):
    # NOTE: trying to add more entries than state can handle,
    # to see if it will clean up and vacuum successfully
    dvc.config.set("state", "row_limit", 10)

    path = tmp_dir / "dir"
    path.mkdir()
    for i in range(20):
        (path / str(i)).write_text(str(i))

    dvc.add("dir")


def mock_get_inode(inode):
    def get_inode_mocked(path):
        return inode

    return get_inode_mocked


@mock.patch("dvc.state.get_inode", autospec=True)
def test_get_state_record_for_inode(get_inode_mock, tmp_dir, dvc):
    tmp_dir.gen("foo", "foo content")

    state = State(dvc, dvc.config.config)
    inode = state.MAX_INT + 2
    assert inode != state._to_sqlite(inode)

    foo = tmp_dir / "foo"
    md5 = file_md5(foo)[0]
    get_inode_mock.side_effect = mock_get_inode(inode)

    with state:
        state.save(PathInfo(foo), md5)
        ret = state.get_state_record_for_inode(inode)
        assert ret is not None


def test_remove_unused_links(tmp_dir, dvc):
    assert len(tmp_dir.dvc_gen("foo", "foo_content")) == 1
    assert len(tmp_dir.dvc_gen("bar", "bar_content")) == 1

    cmd_count_links = "SELECT count(*) FROM {}".format(State.LINK_STATE_TABLE)
    with dvc.state:
        result = dvc.state._execute(cmd_count_links).fetchone()[0]
        assert result == 2

        dvc.state.remove_unused_links([])

        result = dvc.state._execute(cmd_count_links).fetchone()[0]
        assert result == 0

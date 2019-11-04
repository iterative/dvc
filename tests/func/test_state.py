import os

import mock

from dvc.main import main
from dvc.path_info import PathInfo
from dvc.state import State
from dvc.utils import file_md5
from dvc.utils.compat import str


def test_state(dvc_repo, repo_dir):
    path = os.path.join(dvc_repo.root_dir, repo_dir.FOO)
    path_info = PathInfo(path)
    md5 = file_md5(path)[0]

    state = State(dvc_repo, dvc_repo.config.config)

    with state:
        state.save(path_info, md5)
        entry_md5 = state.get(path_info)
        assert entry_md5 == md5

        os.unlink(path)
        with open(path, "a") as fd:
            fd.write("1")

        entry_md5 = state.get(path_info)
        assert entry_md5 is None

        md5 = file_md5(path)[0]
        state.save(path_info, md5)

        entry_md5 = state.get(path_info)
        assert entry_md5 == md5


def test_state_overflow(dvc_repo):
    # NOTE: trying to add more entries than state can handle,
    # to see if it will clean up and vacuum successfully
    ret = main(["config", "state.row_limit", "10"])
    assert ret == 0

    dname = "dir"
    os.mkdir(dname)
    for i in range(20):
        with open(os.path.join(dname, str(i)), "w+") as fobj:
            fobj.write(str(i))

    ret = main(["add", "dir"])
    assert ret == 0


def mock_get_inode(inode):
    def get_inode_mocked(path):
        return inode

    return get_inode_mocked


@mock.patch("dvc.state.get_inode", autospec=True)
def test_get_state_record_for_inode(get_inode_mock, dvc_repo, repo_dir):
    state = State(dvc_repo, dvc_repo.config.config)
    inode = state.MAX_INT + 2
    assert inode != state._to_sqlite(inode)

    path = os.path.join(dvc_repo.root_dir, repo_dir.FOO)
    md5 = file_md5(path)[0]
    get_inode_mock.side_effect = mock_get_inode(inode)

    with state:
        state.save(PathInfo(path), md5)
        ret = state.get_state_record_for_inode(inode)
        assert ret is not None


def test_remove_unused_links(repo_dir, dvc_repo):
    stages = dvc_repo.add(repo_dir.FOO)
    assert len(stages) == 1

    stages = dvc_repo.add(repo_dir.BAR)
    assert len(stages) == 1

    cmd_count_links = "SELECT count(*) FROM {}".format(State.LINK_STATE_TABLE)
    with dvc_repo.state:
        result = dvc_repo.state._execute(cmd_count_links).fetchone()[0]
        assert result == 2

        dvc_repo.state.remove_unused_links([])

        result = dvc_repo.state._execute(cmd_count_links).fetchone()[0]
        assert result == 0

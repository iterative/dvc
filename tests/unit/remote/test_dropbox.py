import multiprocessing
import random
import string
import uuid

import pytest

from dvc.exceptions import DvcException
from dvc.path_info import PathInfo
from dvc.tree.dropbox import DropboxTree


def rand_content(nchar):
    letters = string.ascii_lowercase
    prefix = "".join(random.choice(letters) for _ in range(100))
    return prefix + "a" * (nchar - 100)


SCENARIOS = [
    # (n_files, filesize)
    (100, 100 * 1024),
    # 200 MB is over their limit for single-call upload
    (5, 200 * 1024 * 1024),
]
IDS = [
    "small_files",
    "normal_files",
]


def push(dvc):
    dvc.push()


class TestRemoteDropbox:
    CONFIG = {
        "url": "dropbox://dvc/tests-root",
        "chunk_size": 48,
    }
    INVALID_CHUNK_SIZE_CONFIG_MULTIPLE = {
        "url": "dropbox://dvc/tests-root",
        "chunk_size": 47,
    }
    INVALID_CHUNK_SIZE_CONFIG_LESS_THAN_4 = {
        "url": "dropbox://dvc/tests-root",
        "chunk_size": 0,
    }
    INVALID_CHUNK_SIZE_CONFIG_OVER_150 = {
        "url": "dropbox://dvc/tests-root",
        "chunk_size": 300,
    }

    def test_init(self, dvc, dropbox):
        tree = DropboxTree(dvc, self.CONFIG)
        assert str(tree.path_info) == self.CONFIG["url"]
        assert tree.chunk_size_mb == self.CONFIG["chunk_size"]

        with pytest.raises(DvcException):
            DropboxTree(dvc, self.INVALID_CHUNK_SIZE_CONFIG_MULTIPLE)

        with pytest.raises(DvcException):
            DropboxTree(dvc, self.INVALID_CHUNK_SIZE_CONFIG_LESS_THAN_4)

        with pytest.raises(DvcException):
            DropboxTree(dvc, self.INVALID_CHUNK_SIZE_CONFIG_OVER_150)

    def test_dropbox(self, dvc, tmp_dir, dropbox):
        tree = DropboxTree(dvc, dropbox.config)
        tmp_dir.gen("small", "small")
        to_info = tree.path_info / "small"
        tree.upload(PathInfo("small"), to_info)
        assert tree.exists(to_info)
        hash_info = tree.get_file_hash(to_info)
        assert hash_info.name == "content_hash"
        hash_ = hash_info.value
        assert hash_
        assert isinstance(hash_, str)
        assert hash_.strip("'").strip('"') == hash_

        to_other_info = tree.path_info / "foo" / "bar"
        tree.upload(PathInfo("small"), to_other_info)
        files = list(tree.walk_files(tree.path_info))

        assert len(files) == 2
        assert str(tree.path_info) + "/small" in files
        assert str(tree.path_info) + "/foo/bar" in files

        tree.remove(to_info)
        assert not tree.exists(to_info)

        tmp_dir.gen("large", "large" * 1_000_000)
        tree.upload(PathInfo("large"), tree.path_info / "large")
        assert tree.exists(tree.path_info / "large")

        tree.remove(tree.path_info)

    @pytest.mark.skip(reason="For manual testing only")
    @pytest.mark.parametrize("n_files, size", SCENARIOS, ids=IDS)
    def test_dropbox_files_upload(self, n_files, size, dvc, tmp_dir, dropbox):
        random.seed(42)

        tmp_dir.dvc_gen(
            {
                str(uuid.uuid4()): {
                    "file_{0}".format(i): rand_content(size)
                    for i in range(n_files)
                }
            }
        )

        tmp_dir.add_remote(config=dropbox.config)
        dvc.push()

        tree = DropboxTree(dvc, dropbox.config)
        tree.remove(tree.path_info)

    @pytest.mark.skip(reason="For manual testing only")
    @pytest.mark.parametrize("n_files, size", SCENARIOS, ids=IDS)
    def test_dropbox_recovers_from_failure(
        self, n_files, size, dvc, tmp_dir, scm, dropbox
    ):
        random.seed(42)

        tmp_dir.dvc_gen(
            {
                str(uuid.uuid4()): {
                    "file_{0}".format(i): rand_content(size)
                    for i in range(n_files)
                }
            }
        )

        tmp_dir.add_remote(config=dropbox.config)

        p = multiprocessing.Process(target=push, args=(dvc,))
        p.start()
        p.join(10)  # Let's say we can't upload whole content in 10 secs.
        if p.is_alive():
            p.terminate()
            p.join()

        dvc.push()

        tree = DropboxTree(dvc, dropbox.config)
        tree.remove(tree.path_info)

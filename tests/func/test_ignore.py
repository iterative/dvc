import os
import shutil


from dvc.ignore import DvcIgnore
from dvc.repo import Repo
from dvc.utils.compat import cast_bytes
from dvc.utils.fs import get_mtime_and_size
from tests.basic_env import TestDvc
from tests.utils import to_posixpath


class TestDvcIgnore(TestDvc):
    def setUp(self):
        super(TestDvcIgnore, self).setUp()

    def _get_all_paths(self):

        paths = []
        for root, dirs, files in self.dvc.tree.walk(
            self.dvc.root_dir, dvcignore=self.dvc.dvcignore
        ):
            for dname in dirs:
                paths.append(os.path.join(root, dname))

            for fname in files:
                paths.append(os.path.join(root, fname))

        return paths

    def _reload_dvc(self):
        self.dvc = Repo(self.dvc.root_dir)

    def test_ignore_in_child_dir(self):
        ignore_file = os.path.join(self.dvc.root_dir, DvcIgnore.DVCIGNORE_FILE)
        with open(ignore_file, "w") as fobj:
            fobj.write("data_dir/data")
        self._reload_dvc()

        forbidden_path = os.path.join(self.dvc.root_dir, self.DATA)
        all_paths = self._get_all_paths()

        self.assertNotIn(forbidden_path, all_paths)

    def test_ignore_in_child_dir_unicode(self):
        ignore_file = os.path.join(self.dvc.root_dir, DvcIgnore.DVCIGNORE_FILE)
        with open(ignore_file, "wb") as fobj:
            fobj.write(cast_bytes(self.UNICODE, "utf-8"))
        self._reload_dvc()

        forbidden_path = os.path.join(self.dvc.root_dir, self.UNICODE)
        all_paths = self._get_all_paths()

        self.assertNotIn(forbidden_path, all_paths)

    def test_ignore_in_parent_dir(self):
        ignore_file = os.path.join(self.dvc.root_dir, DvcIgnore.DVCIGNORE_FILE)
        with open(ignore_file, "w") as fobj:
            fobj.write("data_dir/data")
        self._reload_dvc()

        os.chdir(self.DATA_DIR)

        forbidden_path = os.path.join(self.dvc.root_dir, self.DATA)
        all_paths = self._get_all_paths()

        self.assertNotIn(forbidden_path, all_paths)


def test_metadata_unchanged_when_moving_ignored_file(dvc_repo, repo_dir):
    new_data_path = repo_dir.DATA_SUB + "_new"

    ignore_file = os.path.join(dvc_repo.root_dir, DvcIgnore.DVCIGNORE_FILE)
    repo_dir.create(
        ignore_file,
        "\n".join(
            [to_posixpath(repo_dir.DATA_SUB), to_posixpath(new_data_path)]
        ),
    )
    dvc_repo = Repo(dvc_repo.root_dir)

    mtime_sig, size = get_mtime_and_size(
        os.path.abspath(repo_dir.DATA_DIR), dvc_repo.dvcignore
    )

    shutil.move(repo_dir.DATA_SUB, new_data_path)

    new_mtime_sig, new_size = get_mtime_and_size(
        os.path.abspath(repo_dir.DATA_DIR), dvc_repo.dvcignore
    )

    assert new_mtime_sig == mtime_sig
    assert new_size == size


def test_mtime_changed_when_moving_non_ignored_file(dvc_repo, repo_dir):
    new_data_path = repo_dir.DATA_SUB + "_new"
    mtime, size = get_mtime_and_size(repo_dir.DATA_DIR)

    shutil.move(repo_dir.DATA_SUB, new_data_path)
    new_mtime, new_size = get_mtime_and_size(repo_dir.DATA_DIR)

    assert new_mtime != mtime
    assert new_size == size


def test_metadata_unchanged_on_ignored_file_deletion(dvc_repo, repo_dir):
    ignore_file = os.path.join(dvc_repo.root_dir, DvcIgnore.DVCIGNORE_FILE)
    repo_dir.create(ignore_file, to_posixpath(repo_dir.DATA_SUB))
    dvc_repo = Repo(dvc_repo.root_dir)
    mtime, size = get_mtime_and_size(
        os.path.abspath(repo_dir.DATA_DIR), dvc_repo.dvcignore
    )

    os.remove(repo_dir.DATA_SUB)
    # TODO abspath
    new_mtime, new_size = get_mtime_and_size(
        os.path.abspath(repo_dir.DATA_DIR), dvc_repo.dvcignore
    )

    assert new_mtime == mtime
    assert new_size == size


def test_metadata_changed_on_non_ignored_file_deletion(dvc_repo, repo_dir):
    mtime, size = get_mtime_and_size(repo_dir.DATA_DIR)

    os.remove(repo_dir.DATA_SUB)
    new_mtime_sig, new_size = get_mtime_and_size(repo_dir.DATA_DIR)

    assert new_mtime_sig != mtime
    assert new_size != size


def test_should_ignore_for_external_dependency(dvc_repo, repo_dir):
    external_data_dir = repo_dir.mkdtemp()
    data = os.path.join(external_data_dir, "data_external")
    ignored_file = "data_ignored"
    ignored_full_path = os.path.join(external_data_dir, ignored_file)
    ignore_file = os.path.join(dvc_repo.root_dir, DvcIgnore.DVCIGNORE_FILE)
    repo_dir.create(data, "external_data_content")
    repo_dir.create(ignored_full_path, "ignored_file_content")
    repo_dir.create(ignore_file, "/" + ignored_full_path)
    dvc_repo = Repo(dvc_repo.root_dir)

    stages = dvc_repo.add(external_data_dir)
    assert len(stages) == 1

    outs = stages[0].outs
    assert len(outs) == 1

    out_dir_cache = outs[0].dir_cache
    assert len(out_dir_cache) == 1

    assert out_dir_cache[0]["relpath"] == "data_external"


# TODO ignore tests rely heavily on reloading, maybe we should do something
# about that
# TODO problem: what if dvcignore is below out, someone loads repo, then adds
# dir?
def test_should_not_ignore_on_output_below_out_dir(dvc_repo, repo_dir):
    dvc_repo.add(repo_dir.DATA_DIR)

    ignore_file = os.path.join(repo_dir.DATA_DIR, DvcIgnore.DVCIGNORE_FILE)
    ignore_file_content = os.path.basename(repo_dir.DATA)
    repo_dir.create(ignore_file, ignore_file_content)
    dvc_repo = Repo(dvc_repo.root_dir)

    assert dvc_repo.status() == {}


def test_ignore_should_not_raise_on_ignore_in_dependency_dir(
    dvc_repo, repo_dir
):
    ignore_file = os.path.join(repo_dir.DATA_DIR, DvcIgnore.DVCIGNORE_FILE)
    ignore_file_content = os.path.basename(repo_dir.DATA)
    repo_dir.create(ignore_file, ignore_file_content)

    stage_file = "stage.dvc"
    dvc_repo.run(fname=stage_file, deps=[repo_dir.DATA_DIR])

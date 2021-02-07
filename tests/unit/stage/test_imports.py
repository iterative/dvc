from collections import namedtuple

from dvc.path_info import PathInfo
from dvc.stage.imports import get_dir_changes, update_import_dir


class MockTree:
    def __init__(self, files):
        self.files = files

    def get_file_hash(self, file):
        result = namedtuple("HashInfo", "value")
        return result(file)

    def walk_files(self, *args, **kwargs):
        return iter(self.files)

    def download(self, *args, **kwargs):
        pass

    def remove(self, *args, **kwargs):
        pass


class MockStage:
    class TestOut:
        def __init__(self, path_info=None, tree=None):
            self.path_info = path_info
            self.tree = tree

        def update(self, *args, **kwargs):
            pass

    def __init__(self, out, dep, dep_path=None, out_path=None):
        self.outs = [self.TestOut(path_info=out_path, tree=out)]
        self.deps = [self.TestOut(path_info=dep_path, tree=dep)]

    def save_deps(self, *args, **kwargs):
        pass


def test_get_dir_changes():
    out_tree = MockTree(["file1", "file2", "file3"])
    dep_tree = MockTree(["file1", "file2", "file4"])
    stage = MockStage(out_tree, dep_tree)
    files_to_down, files_to_rem = get_dir_changes(stage)
    assert files_to_down == ["file4"]
    assert files_to_rem == ["file3"]


def test_update_import_dir(mocker):
    out_tree = MockTree([])
    dep_tree = MockTree([])
    dep_path = PathInfo("dep/")
    out_path = PathInfo("out/")
    stage = MockStage(out_tree, dep_tree, dep_path=dep_path, out_path=out_path)
    save_deps = mocker.patch.object(stage, "save_deps")
    remove = mocker.patch.object(stage.outs[0].tree, "remove")
    download = mocker.patch.object(stage.deps[0].tree, "download")
    with mocker.patch(
        "dvc.stage.imports.get_dir_changes",
        return_value=([PathInfo("data/file3")], ["file4"]),
    ):
        update_import_dir(stage)
        assert save_deps.called_once_with()
        assert remove.called_once_with("file4")
        assert download.called_once_with("data/file3", "data/file3", jobs=None)

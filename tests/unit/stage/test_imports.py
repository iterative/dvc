from dvc.path_info import PathInfo
from dvc.stage.imports import get_dir_changes, update_import_dir


def test_get_dir_changes(tmp_dir, dvc):

    imp = tmp_dir.gen({"datadir": {"foo": "foo", "bar": "bar"}})[0]

    out = tmp_dir / "out"

    stage = dvc.imp_url(str(imp), str(out))

    (tmp_dir / "datadir" / "foo").write_text("test")
    (tmp_dir / "datadir" / "foo1").write_text("test_1")

    files_to_down, files_to_rem = get_dir_changes(stage)
    assert tmp_dir / "datadir" / "foo" in files_to_down
    assert tmp_dir / "datadir" / "foo1" in files_to_down
    assert tmp_dir / "out" / "foo" in files_to_rem


def test_update_import_dir(tmp_dir, dvc, mocker):
    imp = tmp_dir.gen({"data": {"file3": "foo", "file4": "bar"}})[0]

    out = tmp_dir / "out"

    stage = dvc.imp_url(str(imp), str(out))
    save_deps = mocker.patch.object(stage, "save_deps")
    remove = mocker.patch.object(stage.outs[0].fs, "remove")
    download = mocker.patch.object(stage.deps[0].fs, "download")
    with mocker.patch(
        "dvc.stage.imports.get_dir_changes",
        return_value=([PathInfo("data/file3")], [PathInfo("out/file4")]),
    ):
        update_import_dir(stage)
        assert save_deps.called_once_with()
        assert remove.called_once_with(PathInfo("data/file3"))
        assert download.called_once_with(
            PathInfo("data/file3"), PathInfo("data/file3"), jobs=None
        )

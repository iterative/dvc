import os

import pytest

from dvc.annotations import Annotation
from dvc.dvcfile import SingleStageFile
from dvc.fs import LocalFileSystem
from dvc.output import Output
from dvc.repo import Repo, lock_repo
from dvc.stage import PipelineStage, Stage
from dvc.stage.run import run_stage
from dvc.stage.utils import compute_md5
from dvc.utils import dict_md5
from dvc.utils.serialize import dump_yaml, load_yaml
from dvc.utils.strictyaml import YAMLValidationError


def test_cmd_obj():
    with pytest.raises(YAMLValidationError):
        SingleStageFile.validate({Stage.PARAM_CMD: {}})


def test_cmd_none():
    SingleStageFile.validate({Stage.PARAM_CMD: None})


def test_no_cmd():
    SingleStageFile.validate({})


def test_cmd_str():
    SingleStageFile.validate({Stage.PARAM_CMD: "cmd"})


def test_object():
    with pytest.raises(YAMLValidationError):
        SingleStageFile.validate({Stage.PARAM_DEPS: {}})

    with pytest.raises(YAMLValidationError):
        SingleStageFile.validate({Stage.PARAM_OUTS: {}})


def test_none():
    SingleStageFile.validate({Stage.PARAM_DEPS: None})
    SingleStageFile.validate({Stage.PARAM_OUTS: None})


def test_empty_list():
    d = {Stage.PARAM_DEPS: []}
    SingleStageFile.validate(d)

    d = {Stage.PARAM_OUTS: []}
    SingleStageFile.validate(d)


def test_list():
    lst = [
        {Output.PARAM_PATH: "foo", LocalFileSystem.PARAM_CHECKSUM: "123"},
        {Output.PARAM_PATH: "bar", LocalFileSystem.PARAM_CHECKSUM: None},
        {Output.PARAM_PATH: "baz"},
    ]
    d = {Stage.PARAM_DEPS: lst}
    SingleStageFile.validate(d)

    lst[0][Output.PARAM_CACHE] = True
    lst[1][Output.PARAM_CACHE] = False
    d = {Stage.PARAM_OUTS: lst}
    SingleStageFile.validate(d)


def test_reload(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    d = load_yaml(stage.relpath)

    # NOTE: checking that reloaded stage didn't change its checksum
    md5 = "11111111111111111111111111111111"
    d[stage.PARAM_MD5] = md5
    dump_yaml(stage.relpath, d)

    dvcfile = SingleStageFile(dvc, stage.relpath)
    stage = dvcfile.stage

    assert stage is not None
    dvcfile.dump(stage)

    d = load_yaml(stage.relpath)
    assert d[stage.PARAM_MD5] == md5


def test_default_wdir_ignored_in_checksum(tmp_dir, dvc):
    tmp_dir.gen("bar", "bar")
    stage = dvc.run(
        cmd="cp bar foo",
        deps=["bar"],
        outs=["foo"],
        single_stage=True,
    )

    d = stage.dumpd()
    assert Stage.PARAM_WDIR not in d.keys()

    d = load_yaml(stage.relpath)
    assert Stage.PARAM_WDIR not in d.keys()

    with dvc.lock:
        stage = SingleStageFile(dvc, stage.relpath).stage
        assert not stage.changed()


def test_external_remote_output_resolution(tmp_dir, dvc, make_remote):
    tmp_path = make_remote("tmp", default=False)
    tmp_dir.add_remote(
        url="remote://tmp/storage", name="storage", default=False
    )
    storage = tmp_path / "storage"
    storage.mkdir()
    file_path = storage / "file"

    dvc.run(
        cmd=f"echo file > {file_path}",
        outs_no_cache=["remote://storage/file"],
        single_stage=True,
    )
    assert os.path.exists(file_path)


def test_external_remote_dependency_resolution(tmp_dir, dvc, make_remote):
    tmp_path = make_remote("tmp", default=False)
    tmp_dir.add_remote(
        url="remote://tmp/storage", name="storage", default=False
    )
    storage = tmp_path / "storage"
    storage.mkdir()
    file_path = storage / "file"
    file_path.write_text("Isle of Dogs", encoding="utf-8")

    dvc.imp_url("remote://storage/file", "movie.txt")
    assert (tmp_dir / "movie.txt").read_text() == "Isle of Dogs"


def test_md5_ignores_comments(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen("foo", "foo content")

    with open(stage.path, "a", encoding="utf-8") as f:
        f.write("# End comment\n")

    new_stage = SingleStageFile(dvc, stage.path).stage
    assert not new_stage.changed_stage()


def test_md5_ignores_annotations(tmp_dir, dvc):
    data = {
        "desc": "stage desc",
        "meta": {"key1": "value1", "key2": "value2"},
        "outs": [
            {
                "md5": "d3b07384d113edec49eaa6238ad5ff00",
                "size": 4,
                "path": "foo",
                "desc": "foo desc",
                "type": "mytype",
                "labels": ["get-started", "dataset-registry"],
                "meta": {"key1": "value1"},
            }
        ],
    }
    (tmp_dir / "foo.dvc").dump(data)
    stage = dvc.stage.load_one("foo.dvc")
    assert compute_md5(stage) == "1822617147b53ae6f9eb4b3c87c0b6f3"
    assert (
        dict_md5(
            {
                "outs": [
                    {"md5": "d3b07384d113edec49eaa6238ad5ff00", "path": "foo"}
                ]
            }
        )
        == "1822617147b53ae6f9eb4b3c87c0b6f3"
    )


def test_meta_desc_is_preserved(tmp_dir, dvc):
    data = {
        "desc": "stage desc",
        "meta": {"key1": "value1", "key2": "value2"},
        "outs": [
            {
                "md5": "d3b07384d113edec49eaa6238ad5ff00",
                "size": 4,
                "path": "foo",
                "desc": "foo desc",
                "type": "mytype",
                "labels": ["get-started", "dataset-registry"],
                "meta": {"key": "value"},
            }
        ],
    }
    (tmp_dir / "foo.dvc").dump(data)
    stage = dvc.stage.load_one("foo.dvc")

    assert stage.meta == {"key1": "value1", "key2": "value2"}
    assert stage.desc == "stage desc"
    assert stage.outs[0].annot == Annotation(
        desc="foo desc",
        type="mytype",
        labels=["get-started", "dataset-registry"],
        meta={"key": "value"},
    )

    # sanity check
    stage.dump()
    assert (tmp_dir / "foo.dvc").parse() == data


def test_parent_repo_collect_stages(tmp_dir, scm, dvc):
    tmp_dir.gen({"subdir": {}})
    tmp_dir.gen({"deep": {"dir": {}}})
    subrepo_dir = tmp_dir / "subdir"
    deep_subrepo_dir = tmp_dir / "deep" / "dir"

    with subrepo_dir.chdir():
        subrepo = Repo.init(subdir=True)
        subrepo_dir.gen("subrepo_file", "subrepo file content")
        subrepo.add("subrepo_file")

    with deep_subrepo_dir.chdir():
        deep_subrepo = Repo.init(subdir=True)
        deep_subrepo_dir.gen("subrepo_file", "subrepo file content")
        deep_subrepo.add("subrepo_file")

    dvc._reset()

    stages = dvc.stage.collect(None)
    subrepo_stages = subrepo.stage.collect(None)
    deep_subrepo_stages = deep_subrepo.stage.collect(None)

    assert stages == []
    assert subrepo_stages != []
    assert deep_subrepo_stages != []


def test_collect_repo_ignored_dir_unignored_pattern(tmp_dir, dvc, scm):
    tmp_dir.gen({".gitignore": "data/**\n!data/**/\n!data/**/*.dvc"})
    scm.add([".gitignore"])
    (stage,) = tmp_dir.dvc_gen({"data/raw/tracked.csv": "5,6,7,8"})
    assert dvc.stage.collect_repo() == [stage]


@pytest.mark.parametrize("with_deps", (False, True))
def test_collect_symlink(tmp_dir, dvc, with_deps):
    tmp_dir.gen({"data": {"foo": "foo contents"}})
    foo_path = os.path.join("data", "foo")
    dvc.add(foo_path)

    data_link = tmp_dir / "data_link"
    data_link.symlink_to("data")
    stage = list(
        dvc.stage.collect(
            target=str(data_link / "foo.dvc"), with_deps=with_deps
        )
    )[0]

    assert stage.addressing == f"{foo_path}.dvc"


def test_stage_strings_representation(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    stage1 = run_copy("foo", "bar", single_stage=True)
    assert stage1.addressing == "bar.dvc"
    assert repr(stage1) == "Stage: 'bar.dvc'"
    assert str(stage1) == "stage: 'bar.dvc'"

    stage2 = run_copy("bar", "baz", name="copy-bar-baz")
    assert stage2.addressing == "copy-bar-baz"
    assert repr(stage2) == "Stage: 'copy-bar-baz'"
    assert str(stage2) == "stage: 'copy-bar-baz'"

    folder = tmp_dir / "dir"
    folder.mkdir()
    with folder.chdir():
        # `Stage` caches `relpath` results, forcing it to reset
        stage1.path = stage1.path
        stage2.path = stage2.path

        rel_path = os.path.relpath(stage1.path)
        assert stage1.addressing == rel_path
        assert repr(stage1) == f"Stage: '{rel_path}'"
        assert str(stage1) == f"stage: '{rel_path}'"

        rel_path = os.path.relpath(stage2.path)
        assert stage2.addressing == f"{rel_path}:{stage2.name}"
        assert repr(stage2) == f"Stage: '{rel_path}:{stage2.name}'"
        assert str(stage2) == f"stage: '{rel_path}:{stage2.name}'"


def test_stage_on_no_path_string_repr(tmp_dir, dvc):
    s = Stage(dvc)
    assert s.addressing == "No path"
    assert repr(s) == "Stage: 'No path'"
    assert str(s) == "stage: 'No path'"

    p = PipelineStage(dvc, name="stage_name")
    assert p.addressing == "No path:stage_name"
    assert repr(p) == "Stage: 'No path:stage_name'"
    assert str(p) == "stage: 'No path:stage_name'"


def test_stage_remove_pipeline_stage(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar")
    run_copy("bar", "foobar", name="copy-bar-foobar")

    dvc_file = stage.dvcfile
    with dvc.lock:
        stage.remove(purge=False)
    assert stage.name in dvc_file.stages

    with dvc.lock:
        stage.remove()
    assert stage.name not in dvc_file.stages
    assert "copy-bar-foobar" in dvc_file.stages


def test_stage_remove_pointer_stage(tmp_dir, dvc, run_copy):
    (stage,) = tmp_dir.dvc_gen("foo", "foo")

    with dvc.lock:
        stage.remove(purge=False)
    assert not (tmp_dir / "foo").exists()
    assert (tmp_dir / stage.relpath).exists()

    with dvc.lock:
        stage.remove()
    assert not (tmp_dir / stage.relpath).exists()


@pytest.mark.parametrize("checkpoint", [True, False])
def test_stage_run_checkpoint(tmp_dir, dvc, mocker, checkpoint):
    stage = Stage(dvc, "stage.dvc", cmd="mycmd arg1 arg2")
    mocker.patch.object(stage, "save")

    mock_cmd_run = mocker.patch("dvc.stage.run.cmd_run")
    if checkpoint:
        callback = mocker.Mock()
    else:
        callback = None

    with lock_repo(dvc):
        run_stage(stage, checkpoint_func=callback)
    mock_cmd_run.assert_called_with(
        stage, checkpoint_func=callback, dry=False, run_env=None
    )

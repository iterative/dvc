import os
from operator import itemgetter

import pytest

from dvc.cache import Cache
from dvc.dvcfile import PIPELINE_FILE, PIPELINE_LOCK
from dvc.exceptions import NoOutputOrStageError
from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.stage.exceptions import (
    StageFileDoesNotExistError,
    StageNameUnspecified,
    StageNotFound,
)
from dvc.system import System
from dvc.utils import relpath
from dvc.utils.fs import remove


def test_destroy(tmp_dir, dvc, run_copy):
    dvc.config["cache"]["type"] = ["symlink"]
    dvc.cache = Cache(dvc)

    tmp_dir.dvc_gen("file", "text")
    tmp_dir.dvc_gen({"dir": {"file": "lorem", "subdir/file": "ipsum"}})

    run_copy("file", "file2", single_stage=True)
    run_copy("file2", "file3", name="copy-file2-file3")
    run_copy("file3", "file4", name="copy-file3-file4")

    dvc.destroy()

    # Remove all the files related to DVC
    for path in [
        ".dvc",
        "file.dvc",
        "file2.dvc",
        "dir.dvc",
        PIPELINE_FILE,
        PIPELINE_LOCK,
    ]:
        assert not (tmp_dir / path).exists()

    # Leave the rest of the files
    for path in [
        "file",
        "file2",
        "file3",
        "file4",
        "dir/file",
        "dir/subdir/file",
    ]:
        assert (tmp_dir / path).is_file()

    # Make sure that data was unprotected after `destroy`
    for path in [
        "file",
        "file2",
        "file3",
        "file4",
        "dir",
        "dir/file",
        "dir/subdir",
        "dir/subdir/file",
    ]:
        assert not System.is_symlink(tmp_dir / path)


def test_collect(tmp_dir, scm, dvc, run_copy):
    def collect_outs(*args, **kwargs):
        return {
            str(out)
            for stage in dvc.collect(*args, **kwargs)
            for out in stage.outs
        }

    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", single_stage=True)
    scm.add([".gitignore", "foo.dvc", "bar.dvc"])
    scm.commit("Add foo and bar")

    scm.checkout("new-branch", create_new=True)

    run_copy("bar", "buzz", single_stage=True)
    scm.add([".gitignore", "buzz.dvc"])
    scm.commit("Add buzz")

    assert collect_outs("bar.dvc", with_deps=True) == {"foo", "bar"}
    assert collect_outs("buzz.dvc", with_deps=True) == {"foo", "bar", "buzz"}
    assert collect_outs("buzz.dvc", with_deps=False) == {"buzz"}

    run_copy("foo", "foobar", name="copy-foo-foobar")
    assert collect_outs(":copy-foo-foobar") == {"foobar"}
    assert collect_outs(":copy-foo-foobar", with_deps=True) == {
        "foobar",
        "foo",
    }
    assert collect_outs("dvc.yaml:copy-foo-foobar", recursive=True) == {
        "foobar"
    }
    assert collect_outs("copy-foo-foobar") == {"foobar"}
    assert collect_outs("copy-foo-foobar", with_deps=True) == {
        "foobar",
        "foo",
    }
    assert collect_outs("copy-foo-foobar", recursive=True) == {"foobar"}

    run_copy("foobar", "baz", name="copy-foobar-baz")
    assert collect_outs("dvc.yaml") == {"foobar", "baz"}
    assert collect_outs("dvc.yaml", with_deps=True) == {
        "foobar",
        "baz",
        "foo",
    }


def test_collect_dir_recursive(tmp_dir, dvc, run_head):
    tmp_dir.gen({"dir": {"foo": "foo"}})
    (stage1,) = dvc.add("dir", recursive=True)
    with (tmp_dir / "dir").chdir():
        stage2 = run_head("foo", name="copy-foo-bar")
        stage3 = run_head("foo-1", single_stage=True)
    assert set(dvc.collect("dir", recursive=True)) == {stage1, stage2, stage3}


def test_collect_with_not_existing_output_or_stage_name(
    tmp_dir, dvc, run_copy
):
    with pytest.raises(StageFileDoesNotExistError):
        dvc.collect("some_file")
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    with pytest.raises(StageNotFound):
        dvc.collect("some_file")


def test_stages(tmp_dir, dvc):
    def collect_stages():
        return {stage.relpath for stage in Repo(os.fspath(tmp_dir)).stages}

    tmp_dir.dvc_gen({"file": "a", "dir/file": "b", "dir/subdir/file": "c"})

    assert collect_stages() == {
        "file.dvc",
        os.path.join("dir", "file.dvc"),
        os.path.join("dir", "subdir", "file.dvc"),
    }

    tmp_dir.gen(".dvcignore", "dir")

    assert collect_stages() == {"file.dvc"}


@pytest.fixture
def stages(tmp_dir, run_copy):
    stage1, stage2 = tmp_dir.dvc_gen({"foo": "foo", "lorem": "lorem"})
    return {
        "foo-generate": stage1,
        "lorem-generate": stage2,
        "copy-foo-bar": run_copy("foo", "bar", single_stage=True),
        "copy-bar-foobar": run_copy("bar", "foobar", name="copy-bar-foobar"),
        "copy-lorem-ipsum": run_copy("lorem", "ipsum", name="lorem-ipsum"),
    }


def test_collect_granular_with_no_target(tmp_dir, dvc, stages):
    assert set(map(itemgetter(0), dvc.collect_granular())) == set(
        stages.values()
    )
    assert list(map(itemgetter(1), dvc.collect_granular())) == [None] * len(
        stages
    )


def test_collect_granular_with_target(tmp_dir, dvc, stages):
    assert dvc.collect_granular("bar.dvc") == [(stages["copy-foo-bar"], None)]
    assert dvc.collect_granular(PIPELINE_FILE) == [
        (stages["copy-bar-foobar"], None),
        (stages["copy-lorem-ipsum"], None),
    ]
    assert dvc.collect_granular(":") == [
        (stages["copy-bar-foobar"], None),
        (stages["copy-lorem-ipsum"], None),
    ]
    assert dvc.collect_granular("copy-bar-foobar") == [
        (stages["copy-bar-foobar"], None)
    ]
    assert dvc.collect_granular(":copy-bar-foobar") == [
        (stages["copy-bar-foobar"], None)
    ]
    assert dvc.collect_granular("dvc.yaml:copy-bar-foobar") == [
        (stages["copy-bar-foobar"], None)
    ]

    with (tmp_dir / dvc.DVC_DIR).chdir():
        assert dvc.collect_granular(
            relpath(tmp_dir / PIPELINE_FILE) + ":copy-bar-foobar"
        ) == [(stages["copy-bar-foobar"], None)]

    assert dvc.collect_granular("foobar") == [
        (stages["copy-bar-foobar"], PathInfo(tmp_dir / "foobar"))
    ]


@pytest.mark.parametrize(
    "target",
    [
        "not_existing.dvc",
        "not_existing.dvc:stage_name",
        "not_existing/dvc.yaml",
        "not_existing/dvc.yaml:stage_name",
    ],
)
def test_collect_with_not_existing_dvcfile(tmp_dir, dvc, target):
    with pytest.raises(StageFileDoesNotExistError):
        dvc.collect_granular(target)
    with pytest.raises(StageFileDoesNotExistError):
        dvc.collect(target)


def test_collect_granular_with_not_existing_output_or_stage_name(tmp_dir, dvc):
    with pytest.raises(NoOutputOrStageError):
        dvc.collect_granular("some_file")
    with pytest.raises(NoOutputOrStageError):
        dvc.collect_granular("some_file", recursive=True)


def test_collect_granular_with_deps(tmp_dir, dvc, stages):
    assert set(
        map(itemgetter(0), dvc.collect_granular("bar.dvc", with_deps=True))
    ) == {stages["copy-foo-bar"], stages["foo-generate"]}
    assert set(
        map(
            itemgetter(0),
            dvc.collect_granular("copy-bar-foobar", with_deps=True),
        )
    ) == {
        stages["copy-bar-foobar"],
        stages["copy-foo-bar"],
        stages["foo-generate"],
    }
    assert set(
        map(
            itemgetter(0), dvc.collect_granular(PIPELINE_FILE, with_deps=True),
        )
    ) == set(stages.values())


def test_collect_granular_same_output_name_stage_name(tmp_dir, dvc, run_copy):
    (stage1,) = tmp_dir.dvc_gen("foo", "foo")
    (stage2,) = tmp_dir.dvc_gen("copy-foo-bar", "copy-foo-bar")
    stage3 = run_copy("foo", "bar", name="copy-foo-bar")

    assert dvc.collect_granular("copy-foo-bar") == [(stage3, None)]

    coll = dvc.collect_granular("copy-foo-bar", with_deps=True)
    assert set(map(itemgetter(0), coll)) == {stage3, stage1}
    assert list(map(itemgetter(1), coll)) == [None] * 2

    assert dvc.collect_granular("./copy-foo-bar") == [
        (stage2, PathInfo(tmp_dir / "copy-foo-bar"))
    ]
    assert dvc.collect_granular("./copy-foo-bar", with_deps=True) == [
        (stage2, PathInfo(tmp_dir / "copy-foo-bar"))
    ]


def test_collect_granular_priority_on_collision(tmp_dir, dvc, run_copy):
    tmp_dir.gen({"dir": {"foo": "foo"}, "foo": "foo"})
    (stage1,) = dvc.add("dir", recursive=True)
    stage2 = run_copy("foo", "bar", name="dir")

    assert dvc.collect_granular("dir") == [(stage2, None)]
    assert dvc.collect_granular("dir", recursive=True) == [(stage1, None)]

    remove(tmp_dir / "dir")

    assert dvc.collect_granular("dir") == [(stage2, None)]
    assert dvc.collect_granular("dir", recursive=True) == [(stage2, None)]


def test_collect_granular_collision_output_dir_stage_name(
    tmp_dir, dvc, run_copy
):
    stage1, *_ = tmp_dir.dvc_gen({"dir": {"foo": "foo"}, "foo": "foo"})
    stage3 = run_copy("foo", "bar", name="dir")

    assert dvc.collect_granular("dir") == [(stage3, None)]
    assert not dvc.collect_granular("dir", recursive=True)
    assert dvc.collect_granular("./dir") == [
        (stage1, PathInfo(tmp_dir / "dir"))
    ]


def test_collect_granular_not_existing_stage_name(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    (stage,) = tmp_dir.dvc_gen("copy-foo-bar", "copy-foo-bar")
    run_copy("foo", "bar", name="copy-foo-bar")

    assert dvc.collect_granular("copy-foo-bar.dvc:stage_name_not_needed") == [
        (stage, None)
    ]
    with pytest.raises(StageNotFound):
        dvc.collect_granular("dvc.yaml:does-not-exist")


def test_get_stages(tmp_dir, dvc, run_copy):
    with pytest.raises(StageFileDoesNotExistError):
        dvc.get_stages()

    tmp_dir.gen("foo", "foo")
    stage1 = run_copy("foo", "bar", name="copy-foo-bar")
    stage2 = run_copy("bar", "foobar", name="copy-bar-foobar")

    assert set(dvc.get_stages()) == {stage1, stage2}
    assert set(dvc.get_stages(path=PIPELINE_FILE)) == {stage1, stage2}
    assert set(dvc.get_stages(name="copy-bar-foobar")) == {stage2}
    assert set(dvc.get_stages(path=PIPELINE_FILE, name="copy-bar-foobar")) == {
        stage2
    }

    with pytest.raises(StageFileDoesNotExistError):
        dvc.get_stages(path=relpath(tmp_dir / ".." / PIPELINE_FILE))

    with pytest.raises(StageNotFound):
        dvc.get_stages(path=PIPELINE_FILE, name="copy")


def test_get_stages_old_dvcfile(tmp_dir, dvc):
    (stage1,) = tmp_dir.dvc_gen("foo", "foo")
    assert set(dvc.get_stages("foo.dvc")) == {stage1}
    assert set(dvc.get_stages("foo.dvc", name="foo-generate")) == {stage1}

    with pytest.raises(StageFileDoesNotExistError):
        dvc.get_stages(path=relpath(tmp_dir / ".." / "foo.dvc"))


def test_get_stage(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    stage1 = run_copy("foo", "bar", name="copy-foo-bar")

    with pytest.raises(StageNameUnspecified):
        dvc.get_stage()

    with pytest.raises(StageNameUnspecified):
        dvc.get_stage(path=PIPELINE_FILE)

    assert dvc.get_stage(path=PIPELINE_FILE, name="copy-foo-bar") == stage1
    assert dvc.get_stage(name="copy-foo-bar") == stage1

    with pytest.raises(StageFileDoesNotExistError):
        dvc.get_stage(path="something.yaml", name="name")

    with pytest.raises(StageNotFound):
        dvc.get_stage(name="random_name")


def test_get_stage_single_stage_dvcfile(tmp_dir, dvc):
    (stage1,) = tmp_dir.dvc_gen("foo", "foo")
    assert dvc.get_stage("foo.dvc") == stage1
    assert dvc.get_stage("foo.dvc", name="jpt") == stage1
    with pytest.raises(StageFileDoesNotExistError):
        dvc.get_stage(path="bar.dvc", name="name")

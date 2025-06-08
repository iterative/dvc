from copy import deepcopy

import pytest

from dvc.parsing import DEFAULT_PARAMS_FILE, DataResolver, ResolveError
from dvc.parsing.context import Context
from dvc.utils.serialize import dumps_yaml

from . import CONTEXT_DATA, RESOLVED_DVC_YAML_DATA, TEMPLATED_DVC_YAML_DATA

DATA = {"models": {"bar": "bar", "foo": "foo"}}


def test_resolver(tmp_dir, dvc):
    resolver = DataResolver(dvc, tmp_dir.fs_path, TEMPLATED_DVC_YAML_DATA)
    resolver.context.merge_update(Context(CONTEXT_DATA))
    assert resolver.resolve() == RESOLVED_DVC_YAML_DATA


def test_default_params_file_not_exist(tmp_dir, dvc):
    d = {"vars": [DATA["models"]]}
    resolver = DataResolver(dvc, tmp_dir.fs_path, d)
    assert resolver.context == d["vars"][0]


def test_no_params_yaml_and_vars(tmp_dir, dvc):
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    assert not resolver.context


def test_local_vars(tmp_dir, dvc):
    resolver = DataResolver(
        dvc, tmp_dir.fs_path, {"vars": [{"foo": "bar", "bar": "foo"}]}
    )
    assert resolver.context == {"foo": "bar", "bar": "foo"}


@pytest.mark.parametrize("vars_", ["${file}_params.yaml", {"foo": "${foo}"}])
def test_vars_interpolation_errors(tmp_dir, dvc, vars_):
    with pytest.raises(ResolveError) as exc_info:
        DataResolver(dvc, tmp_dir.fs_path, {"vars": [vars_, {"bar": "foo"}]})
    assert (
        str(exc_info.value)
        == "failed to parse 'vars' in 'dvc.yaml': interpolating is not allowed"
    )


@pytest.mark.parametrize("vars_", [{}, {"vars": []}, {"vars": [DEFAULT_PARAMS_FILE]}])
def test_default_params_file(tmp_dir, dvc, vars_):
    (tmp_dir / DEFAULT_PARAMS_FILE).dump(DATA)
    resolver = DataResolver(dvc, tmp_dir.fs_path, vars_)
    assert resolver.context == DATA


def test_load_vars_from_file(tmp_dir, dvc):
    (tmp_dir / DEFAULT_PARAMS_FILE).dump(DATA)

    datasets = {"datasets": ["foo", "bar"]}
    (tmp_dir / "params.json").dump(datasets)
    d = {"vars": [DEFAULT_PARAMS_FILE, "params.json"]}
    resolver = DataResolver(dvc, tmp_dir.fs_path, d)

    expected = deepcopy(DATA)
    expected.update(datasets)
    assert resolver.context == expected


def test_load_vars_with_relpath(tmp_dir, scm, dvc):
    tmp_dir.scm_gen(DEFAULT_PARAMS_FILE, dumps_yaml(DATA), commit="add params")

    revisions = ["HEAD", "workspace"]
    for rev in dvc.brancher(revs=["HEAD"]):
        assert rev == revisions.pop()
        d = {"vars": [f"../{DEFAULT_PARAMS_FILE}"]}
        resolver = DataResolver(dvc, "subdir", d)
        assert resolver.context == deepcopy(DATA)


def test_partial_vars_doesnot_exist(tmp_dir, dvc):
    (tmp_dir / "test_params.yaml").dump({"sub1": "sub1"})

    with pytest.raises(ResolveError) as exc_info:
        DataResolver(dvc, tmp_dir.fs_path, {"vars": ["test_params.yaml:sub2"]})

    assert (
        str(exc_info.value) == "failed to parse 'vars' in 'dvc.yaml': "
        "could not find 'sub2' in 'test_params.yaml'"
    )


def test_global_overwrite_error_on_imports(tmp_dir, dvc):
    (tmp_dir / DEFAULT_PARAMS_FILE).dump(DATA)
    (tmp_dir / "params.json").dump(DATA)

    d = {"vars": [DEFAULT_PARAMS_FILE, "params.json"]}
    with pytest.raises(ResolveError) as exc_info:
        DataResolver(dvc, tmp_dir.fs_path, d)

    assert (
        str(exc_info.value) == "failed to parse 'vars' in 'dvc.yaml':\n"
        "cannot redefine 'models.bar' from 'params.json' "
        "as it already exists in 'params.yaml'"
    )


def test_global_overwrite_vars(tmp_dir, dvc):
    (tmp_dir / DEFAULT_PARAMS_FILE).dump(DATA)
    d = {"vars": [DATA]}

    with pytest.raises(ResolveError) as exc_info:
        DataResolver(dvc, tmp_dir.fs_path, d)

    assert (
        str(exc_info.value) == "failed to parse 'vars' in 'dvc.yaml':\n"
        "cannot redefine 'models.bar' from 'vars[0]' "
        "as it already exists in 'params.yaml'"
    )


def test_local_declared_vars_overwrite(tmp_dir, dvc):
    (tmp_dir / DEFAULT_PARAMS_FILE).dump(DATA)

    d = {"vars": [DATA["models"], DATA["models"]]}
    with pytest.raises(ResolveError) as exc_info:
        DataResolver(dvc, tmp_dir.fs_path, d)

    assert (
        str(exc_info.value) == "failed to parse 'vars' in 'dvc.yaml':\n"
        "cannot redefine 'bar' from 'vars[1]' "
        "as it already exists in 'vars[0]'"
    )


def test_specified_params_file_not_exist(tmp_dir, dvc):
    d = {"vars": ["not_existing_params.yaml"]}
    with pytest.raises(ResolveError) as exc_info:
        DataResolver(dvc, tmp_dir.fs_path, d)

    assert (
        str(exc_info.value) == "failed to parse 'vars' in 'dvc.yaml': "
        "'not_existing_params.yaml' does not exist"
    )


@pytest.mark.parametrize("local", [True, False])
@pytest.mark.parametrize(
    "vars_",
    [
        ["test_params.yaml", "test_params.yaml:sub1"],
        ["test_params.yaml:sub1", "test_params.yaml"],
        ["test_params.yaml:sub1", "test_params.yaml:sub1,sub2"],
    ],
)
def test_vars_already_loaded_message(tmp_dir, dvc, local, vars_):
    d = {"stages": {"build": {"cmd": "echo ${sub1} ${sub2}"}}}
    (tmp_dir / "test_params.yaml").dump({"sub1": "sub1", "sub2": "sub2"})
    if not local:
        d["vars"] = vars_
    else:
        d["stages"]["build"]["vars"] = vars_

    with pytest.raises(ResolveError) as exc_info:  # noqa: PT012
        resolver = DataResolver(dvc, tmp_dir.fs_path, d)
        resolver.resolve()
    assert "partially" in str(exc_info.value)


@pytest.mark.parametrize(
    "vars_, loc", [(DATA, "build.vars[0]"), ("params.json", "params.json")]
)
def test_local_overwrite_error(tmp_dir, dvc, vars_, loc):
    (tmp_dir / DEFAULT_PARAMS_FILE).dump(DATA)
    (tmp_dir / "params.json").dump(DATA)

    d = {"stages": {"build": {"cmd": "echo ${models.foo}", "vars": [vars_]}}}

    resolver = DataResolver(dvc, tmp_dir.fs_path, d)
    with pytest.raises(ResolveError) as exc_info:
        resolver.resolve()

    assert (
        str(exc_info.value) == "failed to parse stage 'build' in 'dvc.yaml':\n"
        f"cannot redefine 'models.bar' from '{loc}' "
        "as it already exists in 'params.yaml'"
    )

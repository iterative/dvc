import logging
import re

import pytest

from dvc.parsing import DEFAULT_PARAMS_FILE, DataResolver, ResolveError
from dvc.utils.fs import remove
from dvc.utils.serialize import dump_json, dump_yaml

DATA = {"models": {"bar": "bar", "foo": "foo"}}


def escape_ansi(line):
    ansi_escape = re.compile(r"(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]")
    return ansi_escape.sub("", line)


@pytest.fixture
def repo(tmp_dir, dvc):
    dump_yaml(tmp_dir / DEFAULT_PARAMS_FILE, DATA)
    dump_json(tmp_dir / "params.json", DATA)
    yield dvc


def test_global_overwrite_error_on_imports(tmp_dir, repo):
    d = {"vars": [DEFAULT_PARAMS_FILE, "params.json"]}
    with pytest.raises(ResolveError) as exc_info:
        DataResolver(repo, tmp_dir, d)
    assert str(exc_info.value) == (
        "failed to parse 'vars' in 'dvc.yaml':\n"
        "cannot redefine 'models.bar' from 'params.json' "
        "as it already exists in 'params.yaml'"
    )


def test_global_overwrite_vars(tmp_dir, repo):
    d = {"vars": [DATA]}
    with pytest.raises(ResolveError) as exc_info:
        DataResolver(repo, tmp_dir, d)
    assert str(exc_info.value) == (
        "failed to parse 'vars' in 'dvc.yaml':\n"
        "cannot redefine 'models.bar' from 'vars[0]' "
        "as it already exists in 'params.yaml'"
    )


def test_local_declared_vars_overwrite(tmp_dir, repo):
    d = {"vars": [DATA["models"], DATA["models"]]}
    with pytest.raises(ResolveError) as exc_info:
        DataResolver(repo, tmp_dir, d)
    assert str(exc_info.value) == (
        "failed to parse 'vars' in 'dvc.yaml':\n"
        "cannot redefine 'bar' from 'vars[1]' "
        "as it already exists in 'vars[0]'"
    )


def test_default_params_file_not_exist(tmp_dir, repo):
    remove(tmp_dir / DEFAULT_PARAMS_FILE)
    d = {"vars": [DATA["models"]]}
    DataResolver(repo, tmp_dir, d)


def test_specified_params_file_not_exist(tmp_dir, repo):
    d = {"vars": ["not_existing_params.yaml"]}
    with pytest.raises(ResolveError) as exc_info:
        DataResolver(repo, tmp_dir, d)
    assert str(exc_info.value) == (
        "failed to parse 'vars' in 'dvc.yaml': "
        "'not_existing_params.yaml' does not exist"
    )


def test_failed_to_interpolate(tmp_dir, repo):
    d = {
        "vars": [DEFAULT_PARAMS_FILE],
        "stages": {"build": {"cmd": "echo ${models.foo.}"}},
    }
    with pytest.raises(ResolveError) as exc_info:
        resolver = DataResolver(repo, tmp_dir, d)
        resolver.resolve()
    assert escape_ansi(str(exc_info.value)) == (
        "failed to parse 'stages.build.cmd' in 'dvc.yaml':\n"
        "${models.foo.}\n"
        "            ^\n"
        "ParseException: Expected end of text, found '.'"
        "  (at char 12), (line:1, col:13)"
    )


@pytest.mark.parametrize(
    "vars_, loc", [(DATA, "build.vars[0]"), ("params.json", "params.json")],
)
def test_local_overwrite_error(tmp_dir, repo, vars_, loc):
    d = {"stages": {"build": {"cmd": "echo ${models.foo}", "vars": [vars_]}}}
    resolver = DataResolver(repo, tmp_dir, d)
    with pytest.raises(ResolveError) as exc_info:
        resolver.resolve()
    assert str(exc_info.value) == (
        "failed to parse stage 'build' in 'dvc.yaml':\n"
        f"cannot redefine 'models.bar' from '{loc}' "
        "as it already exists in 'params.yaml'"
    )


def test_local_vars_params_file_not_exist(tmp_dir, repo):
    d = {
        "stages": {
            "build": {
                "cmd": "echo ${models.foo}",
                "vars": ["not_existing_params.yaml"],
            }
        }
    }
    resolver = DataResolver(repo, tmp_dir, d)
    with pytest.raises(ResolveError) as exc_info:
        resolver.resolve()
        assert str(exc_info.value) == (
            "failed to parse stage 'build' in 'dvc.yaml': "
            "'not_existing_params.yaml' does not exist"
        )


def test_specified_key_does_not_exist(tmp_dir, repo):
    d = {"stages": {"build": {"cmd": "echo ${models.foobar}"}}}
    resolver = DataResolver(repo, tmp_dir, d)
    with pytest.raises(ResolveError) as exc_info:
        resolver.resolve()
    assert str(exc_info.value) == (
        "failed to parse 'stages.build.cmd' in 'dvc.yaml': "
        "Could not find 'models.foobar'"
    )


@pytest.mark.parametrize(
    "local_vars, expected_text",
    [
        ({"item": "item"}, "item is"),
        ({"key": "key"}, "key is"),
        ({"item": "item", "key": "key"}, "item and key are"),
    ],
)
def test_foreach_item_key_already_exists(
    tmp_dir, repo, caplog, local_vars, expected_text
):
    d = {
        "vars": [local_vars],
        "stages": {"build": {"foreach": "${models}", "do": {}}},
    }
    resolver = DataResolver(repo, tmp_dir, d)

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="dvc.parsing"):
        resolver.resolve()
    assert expected_text in caplog.text
    assert "generated from 'build'" in caplog.text


def test_foreach_interpolation_key_does_not_exist(tmp_dir, repo):
    d = {
        "stages": {"build": {"foreach": "${modelss}", "do": {}}},
    }
    resolver = DataResolver(repo, tmp_dir, d)
    with pytest.raises(ResolveError) as exc_info:
        resolver.resolve()
    assert str(exc_info.value) == (
        "failed to parse 'stages.build.foreach' in 'dvc.yaml': "
        "Could not find 'modelss'"
    )


def test_foreach_interpolation_key_error(tmp_dir, repo):
    d = {
        "stages": {"build": {"foreach": "${models[123}", "do": {}}},
    }
    resolver = DataResolver(repo, tmp_dir, d)
    with pytest.raises(ResolveError) as exc_info:
        resolver.resolve()
    assert escape_ansi(str(exc_info.value)) == (
        "failed to parse 'stages.build.foreach' in 'dvc.yaml':\n"
        "${models[123}\n"
        "        ^\n"
        "ParseException: Expected end of text, found '['"
        "  (at char 8), (line:1, col:9)"
    )


@pytest.mark.parametrize(
    "syn, msg",
    [
        ("${ite}", " Could not find 'ite'"),
        (
            "${ite.}",
            "\n${ite.}\n"
            "     ^\n"
            "ParseException: Expected end of text, found '.'"
            "  (at char 5), (line:1, col:6)",
        ),
    ],
)
def test_foreach_generated_errors(tmp_dir, repo, syn, msg):
    d = {
        "stages": {
            "build": {"foreach": "${models}", "do": {"cmd": f"echo {syn}"}}
        },
    }
    resolver = DataResolver(repo, tmp_dir, d)
    with pytest.raises(ResolveError) as exc_info:
        resolver.resolve()
    assert escape_ansi(str(exc_info.value)) == (
        "failed to parse 'stages.build@bar.cmd' in 'dvc.yaml':" + msg
    )


@pytest.mark.parametrize(
    "vars_, msg",
    [
        (["params-does-not-exist"], " 'params-does-not-exist' does not exist"),
        (
            [DATA],
            "\ncannot redefine 'models.bar' from 'build@bar.vars[0]' "
            f"as it already exists in '{DEFAULT_PARAMS_FILE}'",
        ),
    ],
)
def test_foreach_generated_local_vars_error(tmp_dir, repo, vars_, msg):
    d = {
        "stages": {
            "build": {
                "foreach": "${models}",
                "do": {"vars": vars_, "cmd": "echo ${item}"},
            }
        },
    }
    resolver = DataResolver(repo, tmp_dir, d)
    with pytest.raises(ResolveError) as exc_info:
        resolver.resolve()
    assert str(exc_info.value) == (
        "failed to parse stage 'build@bar' (gen. from 'build') in 'dvc.yaml':"
        + msg
    )


@pytest.mark.parametrize(
    "wdir, msg",
    [
        ("${ite}", " Could not find 'ite'"),
        (
            "${item.}",
            "\n${item.}\n"
            "      ^\n"
            "ParseException: Expected end of text, found '.'"
            "  (at char 6), (line:1, col:7)",
        ),
    ],
)
def test_foreach_wdir_interpolation_issues(tmp_dir, repo, wdir, msg):
    d = {
        "stages": {
            "build": {
                "foreach": "${models}",
                "do": {"wdir": wdir, "cmd": "echo ${item}"},
            }
        },
    }
    resolver = DataResolver(repo, tmp_dir, d)
    with pytest.raises(ResolveError) as exc_info:
        resolver.resolve()
    assert escape_ansi(str(exc_info.value)) == (
        "failed to parse 'stages.build@bar.wdir' in 'dvc.yaml':" + msg
    )


@pytest.mark.parametrize("foreach", ["${models.bar}", "foo"])
def test_foreach_expects_dict_or_list(tmp_dir, repo, foreach):
    d = {
        "stages": {"build": {"foreach": foreach, "do": {}}},
    }
    resolver = DataResolver(repo, tmp_dir, d)
    with pytest.raises(ResolveError) as exc_info:
        resolver.resolve()
    assert str(exc_info.value) == (
        "failed to resolve 'stages.build.foreach' in 'dvc.yaml': "
        "expected list/dictionary, got str"
    )


@pytest.mark.parametrize(
    "wdir, expected_msg",
    [
        ("${models[foobar]}", " Could not find 'models.foobar'"),
        (
            "${models.foo]}",
            "\n${models.foo]}\n"
            "            ^\n"
            "ParseException: Expected end of text, found ']'"
            "  (at char 12), (line:1, col:13)",
        ),
    ],
)
def test_wdir_failed_to_interpolate(tmp_dir, repo, wdir, expected_msg):
    d = {"stages": {"build": {"wdir": wdir, "cmd": "echo ${models.bar}"}}}
    resolver = DataResolver(repo, tmp_dir, d)
    with pytest.raises(ResolveError) as exc_info:
        resolver.resolve()
    assert escape_ansi(str(exc_info.value)) == (
        "failed to parse 'stages.build.wdir' in 'dvc.yaml':" + expected_msg
    )


@pytest.mark.parametrize(
    "foreach, redefine",
    [
        ("${models}", "item"),
        ("${models}", "key"),
        (list(DATA["models"].keys()), "item"),
    ],
)
def test_item_key_in_generated_stage_vars(tmp_dir, repo, foreach, redefine):
    d = {
        "stages": {
            "build": {
                "foreach": foreach,
                "do": {"cmd": "echo ${item}", "vars": [{redefine: "value"}]},
            }
        },
    }
    resolver = DataResolver(repo, tmp_dir, d)
    with pytest.raises(ResolveError) as exc_info:
        resolver.resolve()
    assert str(exc_info.value) == (
        f"attempted to redefine '{redefine}' in stage 'build@bar'"
        " generated through 'foreach'"
    )


def test_interpolate_non_string(tmp_dir, repo):
    d = {"stages": {"build": {"cmd": "echo ${models}"}}}
    resolver = DataResolver(repo, tmp_dir, d)
    with pytest.raises(ResolveError) as exc_info:
        resolver.resolve()
    assert str(exc_info.value) == (
        "failed to parse 'stages.build.cmd' in 'dvc.yaml':\n"
        "Cannot interpolate data of type 'dict'"
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
def test_vars_already_loaded_message(tmp_dir, repo, local, vars_):
    d = {"stages": {"build": {"cmd": "echo ${sub1} ${sub2}"}}}
    dump_yaml("test_params.yaml", {"sub1": "sub1", "sub2": "sub2"})
    if not local:
        d["vars"] = vars_
    else:
        d["stages"]["build"]["vars"] = vars_

    with pytest.raises(ResolveError) as exc_info:
        resolver = DataResolver(repo, tmp_dir, d)
        resolver.resolve()

    assert "partially" in str(exc_info.value)


@pytest.mark.parametrize("local", [False, True])
def test_partial_vars_doesnot_exist(tmp_dir, repo, local):
    d = {"stages": {"build": {"cmd": "echo ${sub1} ${sub2}"}}}
    dump_yaml("test_params.yaml", {"sub1": "sub1", "sub2": "sub2"})
    vars_ = ["test_params.yaml:sub3"]
    if not local:
        d["vars"] = vars_
    else:
        d["stages"]["build"]["vars"] = vars_

    with pytest.raises(ResolveError):
        resolver = DataResolver(repo, tmp_dir, d)
        resolver.resolve()

import os

import pytest
from ruamel.yaml import __with_libyaml__ as ruamel_clib

from dvc.cli import main

DUPLICATE_KEYS = """\
stages:
  stage1:
    cmd: python train.py
    cmd: python train.py
"""

DUPLICATE_KEYS_OUTPUT = """\
'./dvc.yaml' is invalid.

While constructing a mapping, in line 3, column 5
  3 │   cmd: python train.py

Found duplicate key "cmd" with value "python train.py" (original value:\
 "python\ntrain.py"), in line 4, column 5
  4 │   cmd: python train.py"""


MAPPING_VALUES_NOT_ALLOWED = """\
stages:
  stage1
    cmd: python script.py
"""

MAPPING_VALUES_NOT_ALLOWED_OUTPUT = """\
'./dvc.yaml' is invalid.

Mapping values are not allowed {}, in line 3, column 8
  3 │   cmd: python script.py""".format(
    "in this context" if ruamel_clib else "here"
)


NO_HYPHEN_INDICATOR_IN_BLOCK = """\
stages:
  stage1:
    cmd: python script.py
    outs:
      - logs:
          cache: false
      metrics:
"""

NO_HYPHEN_INDICATOR_IN_BLOCK_OUTPUT = """\
'./dvc.yaml' is invalid.

While parsing a block collection, in line 5, column 7
  5 │     - logs:

{}, in line 7, column 7
  7 │     metrics:""".format(
    "Did not find expected '-' indicator"
    if ruamel_clib
    else "Expected <block end>, but found '?'"
)


UNCLOSED_SCALAR = """\
stages:
  stage1:
    cmd: python script.py
    desc: "this is my stage one
"""

UNCLOSED_SCALAR_OUTPUT = """\
'./dvc.yaml' is invalid.

While scanning a quoted scalar, in line 4, column 11
  4 │   desc: "this is my stage one

Found unexpected end of stream, in line 5, column 1
  5"""


NOT_A_DICT = "3"
NOT_A_DICT_OUTPUT = "'./dvc.yaml' validation failed: expected a dictionary.\n"


EMPTY_STAGE = """\
stages:
  stage1:
"""

EMPTY_STAGE_OUTPUT = """\
'./dvc.yaml' validation failed.

expected a dictionary, in stages -> stage1, line 2, column 3
  1 stages:
  2   stage1:
  3"""


MISSING_CMD = """\
stages:
  stage1:
    cmd: {}
"""

MISSING_CMD_OUTPUT = """\
'./dvc.yaml' validation failed.

expected str, in stages -> stage1 -> cmd, line 3, column 10
  2   stage1:
  3 │   cmd: {}"""


DEPS_AS_DICT = """\
stages:
  stage1:
    cmd: python script.py
    deps:
      - src:
"""

DEPS_AS_DICT_OUTPUT = """\
'./dvc.yaml' validation failed.

expected str, in stages -> stage1 -> deps -> 0, line 5, column 9
  4 │   deps:
  5 │     - src:
"""

OUTS_AS_STR = """\
stages:
  train:
    cmd:
      - python train.py
    deps:
      - config.cfg
    outs:
      models/"""

OUTS_AS_STR_OUTPUT = """\
'./dvc.yaml' validation failed.

expected a list, in stages -> train -> outs, line 3, column 5
  2   train:
  3 │   cmd:
  4 │     - python train.py"""


NULL_VALUE_ON_OUTS = """\
stages:
  stage1:
    cmd: python script.py
    outs:
    - logs:
        cache: false
        persist: true
        remote:
"""

NULL_VALUE_ON_OUTS_OUTPUT = """\
'./dvc.yaml' validation failed.

expected str, in stages -> stage1 -> outs -> 0 -> logs -> remote, line 6, \
column\n9
  5 │   - logs:
  6 │   │   cache: false
  7 │   │   persist: true"""

ADDITIONAL_KEY_ON_OUTS = """\
stages:
  stage1:
    cmd: python script.py
    outs:
    - logs:
        cache: false
        not_existing_key: false
"""

ADDITIONAL_KEY_ON_OUTS_OUTPUT = """\
'./dvc.yaml' validation failed.

extra keys not allowed, in stages -> stage1 -> outs -> 0 -> logs ->
not_existing_key, line 6, column 9
  5 │   - logs:
  6 │   │   cache: false
  7 │   │   not_existing_key: false"""


FOREACH_SCALAR_VALUE = """\
stages:
  group:
    foreach: 3
    do:
      cmd: python script${i}.py
"""

FOREACH_SCALAR_VALUE_OUTPUT = """\
'./dvc.yaml' validation failed.

expected dict, in stages -> group -> foreach, line 3, column 5
  2   group:
  3 │   foreach: 3
  4 │   do:"""

FOREACH_DO_NULL = """\
stages:
  stage1:
    foreach: [1,2,3]
    do:
"""


FOREACH_DO_NULL_OUTPUT = """\
'./dvc.yaml' validation failed.

expected a dictionary, in stages -> stage1 -> do, line 3, column 5
  2   stage1:
  3 │   foreach: [1,2,3]
  4 │   do:"""


FOREACH_DO_MISSING_CMD = """\
stages:
  stage1:
    foreach: [1,2,3]
    do:
      outs:
      - ${item}
"""


FOREACH_WITH_CMD_DO_MISSING = """\
stages:
  stage1:
    foreach: [1,2,3]
    cmd: python script${item}.py
"""


FOREACH_WITH_CMD_DO_MISSING_OUTPUT = """\
'./dvc.yaml' validation failed: 2 errors.

extra keys not allowed, in stages -> stage1 -> cmd, line 3, column 5
  2   stage1:
  3 │   foreach: [1,2,3]
  4 │   cmd: python script${item}.py

required key not provided, in stages -> stage1 -> do, line 3, column 5
  2   stage1:
  3 │   foreach: [1,2,3]
  4 │   cmd: python script${item}.py"""


FOREACH_DO_MISSING_CMD_OUTPUT = """\
'./dvc.yaml' validation failed.

required key not provided, in stages -> stage1 -> do -> cmd, line 5, column 7
  4 │   do:
  5 │     outs:
  6 │     - ${item}"""


MERGE_CONFLICTS = """\
stages:
  load_data:
<<<<<<< HEAD
    cmd: python src/load_data.py
    deps:
    - src/load_data.py
=======
    cmd: python load_data.py
    deps:
    - load_data.py
>>>>>>> branch
    outs:
    - data
"""

MERGE_CONFLICTS_OUTPUT = """\
'./dvc.yaml' is invalid (possible merge conflicts).

While scanning a simple key, in line 3, column 1
  3 <<<<<<< HEAD

Could not find expected ':', in line 4, column 8
  4 │   cmd: python src/load_data.py"""


examples = {
    # on parse errors
    "duplicate_keys": (DUPLICATE_KEYS, DUPLICATE_KEYS_OUTPUT),
    "mapping_values_not_allowed": (
        MAPPING_VALUES_NOT_ALLOWED,
        MAPPING_VALUES_NOT_ALLOWED_OUTPUT,
    ),
    "no_hyphen_block": (
        NO_HYPHEN_INDICATOR_IN_BLOCK,
        NO_HYPHEN_INDICATOR_IN_BLOCK_OUTPUT,
    ),
    "unclosed_scalar": (UNCLOSED_SCALAR, UNCLOSED_SCALAR_OUTPUT),
    # schema validation errors
    "not_a_dict": (NOT_A_DICT, NOT_A_DICT_OUTPUT),
    "empty_stage": (EMPTY_STAGE, EMPTY_STAGE_OUTPUT),
    "missing_cmd": (MISSING_CMD, MISSING_CMD_OUTPUT),
    "deps_as_dict": (DEPS_AS_DICT, DEPS_AS_DICT_OUTPUT),
    "outs_as_str": (OUTS_AS_STR, OUTS_AS_STR_OUTPUT),
    "null_value_on_outs": (NULL_VALUE_ON_OUTS, NULL_VALUE_ON_OUTS_OUTPUT),
    "additional_key_on_outs": (
        ADDITIONAL_KEY_ON_OUTS,
        ADDITIONAL_KEY_ON_OUTS_OUTPUT,
    ),
    "foreach_scalar": (FOREACH_SCALAR_VALUE, FOREACH_SCALAR_VALUE_OUTPUT),
    "foreach_do_do_null": (FOREACH_DO_NULL, FOREACH_DO_NULL_OUTPUT),
    "foreach_do_missing_cmd": (
        FOREACH_DO_MISSING_CMD,
        FOREACH_DO_MISSING_CMD_OUTPUT,
    ),
    "foreach_unknown_cmd_missing_do": (
        FOREACH_WITH_CMD_DO_MISSING,
        FOREACH_WITH_CMD_DO_MISSING_OUTPUT,
    ),
    # merge conflicts
    "merge_conflicts": (MERGE_CONFLICTS, MERGE_CONFLICTS_OUTPUT),
}


@pytest.fixture
def force_posixpath(mocker):
    # make it always return posix path, easier for validating error messages
    mocker.patch(
        "dvc.utils.strictyaml.make_relpath",
        return_value="./dvc.yaml",
    )


@pytest.fixture
def fixed_width_term(mocker):
    """Fixed width console."""
    from rich.console import Console

    mocker.patch.object(
        Console, "width", new_callable=mocker.PropertyMock(return_value=80)
    )


@pytest.mark.parametrize(
    "text, expected", examples.values(), ids=examples.keys()
)
def test_exceptions(
    tmp_dir,
    dvc,
    capsys,
    force_posixpath,
    fixed_width_term,
    text,
    expected,
):
    tmp_dir.gen("dvc.yaml", text)

    capsys.readouterr()  # clear outputs
    assert main(["stage", "list"]) != 0
    out, err = capsys.readouterr()

    assert not out

    # strip whitespace on the right: output is always left-justified
    # by rich.syntax.Syntax:
    for expected_line, err_line in zip(
        expected.splitlines(), err.splitlines()
    ):
        assert expected_line == err_line.rstrip(" ")


@pytest.mark.parametrize(
    "text, expected",
    [
        (DUPLICATE_KEYS, "'./dvc.yaml' is invalid in revision '{short_rev}'."),
        (
            MISSING_CMD,
            "'./dvc.yaml' validation failed in revision '{short_rev}'.",
        ),
    ],
)
def test_on_revision(
    tmp_dir,
    scm,
    dvc,
    force_posixpath,
    fixed_width_term,
    capsys,
    text,
    expected,
):
    tmp_dir.scm_gen("dvc.yaml", text, commit="add dvc.yaml")
    capsys.readouterr()  # clear outputs

    assert main(["ls", f"file://{tmp_dir.as_posix()}", "--rev", "HEAD"]) != 0

    out, err = capsys.readouterr()
    assert not out
    assert expected.format(short_rev=scm.get_rev()[:7]) in err


def test_make_relpath(tmp_dir, monkeypatch):
    from dvc.utils.strictyaml import make_relpath

    path = tmp_dir / "dvc.yaml"
    expected_path = "./dvc.yaml" if os.name == "posix" else ".\\dvc.yaml"
    assert make_relpath(path) == expected_path

    (tmp_dir / "dir").mkdir(exist_ok=True)
    monkeypatch.chdir("dir")

    expected_path = "../dvc.yaml" if os.name == "posix" else "..\\dvc.yaml"
    assert make_relpath(path) == expected_path


def test_fallback_exception_message(tmp_dir, dvc, mocker, caplog):
    # When trying to pretty print exception messages, we fallback to old way
    # of printing things.
    mocker.patch(
        "dvc.utils.strictyaml.YAMLSyntaxError.__pretty_exc__",
        side_effect=ValueError,
    )
    mocker.patch(
        "dvc.utils.strictyaml.YAMLValidationError.__pretty_exc__",
        side_effect=ValueError,
    )

    # syntax errors
    dvc_file = tmp_dir / "dvc.yaml"
    dvc_file.write_text(MAPPING_VALUES_NOT_ALLOWED)
    assert main(["stage", "list"]) != 0
    assert (
        "unable to read: 'dvc.yaml', "
        "YAML file structure is corrupted" in caplog.text
    )

    caplog.clear()
    # validation error
    dvc_file.dump({"stages": {"stage1": None}})
    assert main(["stage", "list"]) != 0
    assert "dvc.yaml' validation failed" in caplog.text

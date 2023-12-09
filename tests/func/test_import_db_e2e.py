"""E2E tests involving dvc/dbt and a ~~real~~ sqlite db.

Any other tests should likely be mocked tests in `test_import_db` rather than here.
"""

import os
import sqlite3
from contextlib import closing

import pytest
from agate import Table

from dvc.types import StrPath

pytest.importorskip("dbt.adapters.sqlite", reason="dbt-sqlite not installed")


@pytest.fixture(autouse=True)
def disable_tracking(monkeypatch):
    monkeypatch.setenv("DO_NOT_TRACK", "1")


@pytest.fixture(autouse=True)
def mock_sqlite_adapter(mocker):
    # https://github.com/codeforkjeff/dbt-sqlite/issues/47
    mocker.patch(
        "dbt.adapters.sqlite.connections.SQLiteCredentials.unique_field", "sqlite_host"
    )


@pytest.fixture
def db_path(tmp_dir):
    d = tmp_dir / "db"
    d.mkdir()
    return d / "main.db"


@pytest.fixture
def seed_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE model (id INTEGER PRIMARY KEY, value INTEGER)")

    def inner(values):
        conn.executemany("INSERT INTO model(value) VALUES(?)", [(i,) for i in values])
        conn.commit()

    with closing(conn):
        yield inner


@pytest.fixture
def db_config(dvc, db_path):
    # only needed for `sql_conn` tests
    with dvc.config.edit(level="local") as conf:
        conf["db"] = {"conn": {"url": f"sqlite:///{db_path.fs_path}"}}
    return "conn"


@pytest.fixture
def dbt_profile(tmp_dir, scm, db_path):
    (tmp_dir / "profiles.yml").dump(
        {
            "sqlite_profile": {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "type": "sqlite",
                        "threads": 1,
                        "database": "database",
                        "schema": "main",
                        "schemas_and_paths": {"main": os.fspath(db_path)},
                        "schema_directory": os.fspath(db_path.parent),
                    }
                },
            }
        }
    )
    scm.add_commit(["profiles.yml"], message="add profiles.yml")
    return "sqlite_profile"


@pytest.fixture
def dbt_project(tmp_dir, scm, dbt_profile):
    (tmp_dir / "dbt_project.yml").dump({"name": "project", "profile": dbt_profile})
    scm.add_commit(["dbt_project.yml"], message="add dbt_project")
    return tmp_dir


@pytest.fixture
def dbt_model(dbt_project):
    dbt_project.scm_gen(
        {"models": {"model.sql": "select * from models"}}, commit="add models"
    )
    return "model"


@pytest.fixture(params=("sql", "model", "external_model", "sql_conn"))
def import_db_parameters(request: pytest.FixtureRequest):
    if request.param == "sql":
        profile = request.getfixturevalue("dbt_profile")
        return {"sql": "select * from model", "profile": profile}
    if request.param == "sql_conn":
        conn = request.getfixturevalue("db_config")
        return {"sql": "select * from model", "connection": conn}

    dbt_project = request.getfixturevalue("dbt_project")
    return {
        "model": request.getfixturevalue("dbt_model"),
        "url": None if request.param == "model" else os.fspath(dbt_project),
    }


@pytest.fixture
def file_name(import_db_parameters):
    if model := import_db_parameters.get("model"):
        return model
    return "results"


def load_table(file: StrPath, typ: str) -> "Table":
    return getattr(Table, f"from_{typ}")(file)


@pytest.mark.filterwarnings("ignore::ResourceWarning")  # dbt leaks fileobj from logger
@pytest.mark.parametrize("output_format", ("csv", "json"))
def test_e2e(
    tmp_dir, scm, dvc, seed_db, import_db_parameters, file_name, output_format
):
    seed_db(values=range(5))

    stage = dvc.imp_db(**import_db_parameters, output_format=output_format)

    output_file = tmp_dir / f"{file_name}.{output_format}"
    output = load_table(output_file, output_format)
    assert output.rows == [(i + 1, i) for i in range(5)]

    seed_db(values=range(5, 10))

    dvc.update(stage.addressing)

    output = load_table(output_file, output_format)
    assert output.rows == [(i + 1, i) for i in range(10)]

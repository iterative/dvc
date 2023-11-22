"""E2E tests involving dvc/dbt and a ~~real~~ sqlite db.

Any other tests should likely be mocked tests in `test_import_db` rather than here.
"""

import io
import os
import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from agate import Table


dbt_connections = pytest.importorskip(
    "dbt.adapters.sql.connections", reason="dbt-core not installed"
)
SQLConnectionManager = dbt_connections.SQLConnectionManager
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
        with closing(conn.execute("SELECT * FROM model")) as cursor:
            return SQLConnectionManager.get_result_from_cursor(cursor, None)

    with closing(conn):
        yield inner


@pytest.fixture
def dbt_profile(tmp_dir, db_path):
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
    return "sqlite_profile"


@pytest.fixture
def dbt_project(tmp_dir, dbt_profile):
    (tmp_dir / "dbt_project.yml").dump({"name": "project", "profile": dbt_profile})
    return tmp_dir


@pytest.fixture
def dbt_model(dbt_project):
    dbt_project.gen({"models": {"model.sql": "select * from models"}})
    return "model"


@pytest.fixture(params=("sql", "model"))
def import_db_parameters(request):
    if request.param == "sql":
        profile = request.getfixturevalue("dbt_profile")
        return {"sql": "select * from model", "profile": profile}
    return {"model": request.getfixturevalue("dbt_model")}


@pytest.fixture
def file_name(import_db_parameters):
    if model := import_db_parameters.get("model"):
        return model
    return "results"


def format_output(table: "Table", output_format: str):
    output = io.StringIO()
    getattr(table, f"to_{output_format}")(output)
    return output.getvalue()


@pytest.mark.filterwarnings("ignore::ResourceWarning")  # dbt leaks fileobj from logger
@pytest.mark.parametrize("output_format", ("csv", "json"))
def test_e2e(tmp_dir, dvc, seed_db, import_db_parameters, file_name, output_format):
    results: "Table" = seed_db(values=range(5))

    stage = dvc.imp_db(**import_db_parameters, output_format=output_format)

    output_file = tmp_dir / f"{file_name}.{output_format}"
    assert output_file.read_text() == format_output(results, output_format)

    results: "Table" = seed_db(values=range(6, 10))

    dvc.update(stage.addressing)
    assert output_file.read_text() == format_output(results, output_format)

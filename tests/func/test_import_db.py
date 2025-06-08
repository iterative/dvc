import os
import sqlite3
from contextlib import closing

import pandas as pd
import pytest
from funcy import compact


@pytest.fixture
def db_path(tmp_dir):
    return tmp_dir / "main.db"


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
def db_connection(dvc, db_path):
    with dvc.config.edit(level="local") as conf:
        conf["db"] = {"conn": {"url": f"sqlite:///{db_path.fs_path}"}}
    return "conn"


def load_data(file, output_format):
    if output_format == "json":
        return pd.read_json(file, orient="records")
    return pd.read_csv(file)


@pytest.mark.parametrize("output_format", ("csv", "json"))
@pytest.mark.parametrize(
    "args,file_name",
    [
        ({"sql": "select * from model"}, "results"),
        ({"table": "model"}, "model"),
    ],
)
def test(M, tmp_dir, scm, dvc, db_connection, seed_db, output_format, args, file_name):
    seed_db(values=range(5))
    if output_format == "json":
        file_size = 96, 192
        md5 = "6039fe7565d212b339aaa446ca234e5d", "e1b8adf4d9eb9ab2b64d3ab0bb5f65ac"
    elif os.name == "nt":
        file_size = 35, 61
        md5 = "14c34db5ddd184345c06f74718539f04", "3bb836e6d43c9afa43a9d73b36bbbab4"
    else:
        file_size = 29, 50
        md5 = "6f7fc0d701d1ac13eec83d79fffaf427", "c04f712f8167496a2fb43f289f2b7e28"

    db = compact(
        {
            "file_format": output_format,
            "connection": db_connection,
            "table": args.get("table"),
            "query": args.get("sql"),
        }
    )
    stage = dvc.imp_db(**args, connection=db_connection, output_format=output_format)

    output_file = f"{file_name}.{output_format}"
    df = load_data(output_file, output_format)
    assert df.values.tolist() == [[i + 1, i] for i in range(5)]
    assert (tmp_dir / stage.relpath).parse() == {
        "md5": M.instance_of(str),
        "frozen": True,
        "deps": [{"db": db}],
        "outs": [
            {
                "md5": md5[0],
                "size": file_size[0],
                "hash": "md5",
                "path": output_file,
            }
        ],
    }

    seed_db(values=range(5, 10))

    dvc.update(stage.addressing)

    df = load_data(output_file, output_format)
    assert df.values.tolist() == [[i + 1, i] for i in range(10)]
    assert (tmp_dir / stage.relpath).parse() == {
        "md5": M.instance_of(str),
        "frozen": True,
        "deps": [{"db": db}],
        "outs": [
            {
                "md5": md5[1],
                "size": file_size[1],
                "hash": "md5",
                "path": output_file,
            }
        ],
    }

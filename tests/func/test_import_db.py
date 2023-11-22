"""import_db tests without involving any databases."""

from agate import Table

data1 = Table([(i, i) for i in range(1, 10)], ["id", "value"])
data2 = Table([(i, i) for i in range(10, 20)], ["id", "value"])


def test_sql(mocker, tmp_dir, dvc):
    mocker.patch("dvc.utils.db.execute_sql", side_effect=[data1, data2])
    stage = dvc.imp_db(
        sql="select * from model", profile="profile", output_format="csv"
    )

    db = {"file_format": "csv", "profile": "profile", "query": "select * from model"}
    assert (tmp_dir / stage.relpath).parse() == {
        "md5": "af4810b5c2caaba8397383e5a7f4962f",
        "frozen": True,
        "deps": [{"db": db}],
        "outs": [
            {
                "md5": "b3192d487fc41364dbc7e6f2b09f5018",
                "size": 45,
                "hash": "md5",
                "path": "results.csv",
            }
        ],
    }

    dvc.update(stage.addressing)
    assert (tmp_dir / stage.relpath).parse() == {
        "md5": "72938294b93ab8f66eab8464b13d3b49",
        "frozen": True,
        "deps": [{"db": db}],
        "outs": [
            {
                "md5": "261b6552650139a1de234935ebe1155a",
                "size": 69,
                "hash": "md5",
                "path": "results.csv",
            }
        ],
    }


def test_model(mocker, tmp_dir, dvc):
    mocker.patch("dvc.utils.db.get_model", side_effect=[data1, data2])
    stage = dvc.imp_db(model="model", output_format="csv")

    assert (tmp_dir / stage.relpath).parse() == {
        "md5": "0c398e09fd89633dd296b3ad1bad7d45",
        "frozen": True,
        "deps": [{"db": {"file_format": "csv", "model": "model"}}],
        "outs": [
            {
                "md5": "b3192d487fc41364dbc7e6f2b09f5018",
                "size": 45,
                "hash": "md5",
                "path": "model.csv",
            }
        ],
    }

    dvc.update(stage.addressing)
    assert (tmp_dir / stage.relpath).parse() == {
        "md5": "b542acb4ec4efb0354af6b6e95d3c13a",
        "frozen": True,
        "deps": [{"db": {"file_format": "csv", "model": "model"}}],
        "outs": [
            {
                "md5": "261b6552650139a1de234935ebe1155a",
                "size": 69,
                "hash": "md5",
                "path": "model.csv",
            }
        ],
    }

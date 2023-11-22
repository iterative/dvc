"""import_db tests without involving any databases."""

from agate import Table

data1 = Table([(i, i) for i in range(1, 10)], ["id", "value"])
data2 = Table([(i, i) for i in range(10, 20)], ["id", "value"])


def test_sql(mocker, tmp_dir, dvc):
    mocker.patch("dvc.utils.db.execute_sql", side_effect=[data1, data2])
    stage = dvc.imp_db(
        sql="select * from model", profile="profile", output_format="json"
    )

    db = {"file_format": "json", "profile": "profile", "query": "select * from model"}

    assert (tmp_dir / stage.relpath).parse() == {
        "md5": "dae475048115315c8c26a10538e4b2ec",
        "frozen": True,
        "deps": [{"db": db}],
        "outs": [
            {
                "hash": "md5",
                "md5": "56226d443067b195c90b427db557e1f2",
                "path": "results.json",
                "size": 243,
            }
        ],
    }

    dvc.update(stage.addressing)
    assert (tmp_dir / stage.relpath).parse() == {
        "md5": "79b42f9ddb2a3ed999eb494ccab470c6",
        "frozen": True,
        "deps": [{"db": db}],
        "outs": [
            {
                "hash": "md5",
                "md5": "08d806ee7e1f309c4561750805f02276",
                "path": "results.json",
                "size": 290,
            }
        ],
    }


def test_model(mocker, tmp_dir, dvc):
    mocker.patch("dvc.utils.db.get_model", side_effect=[data1, data2])
    stage = dvc.imp_db(model="model", output_format="json")

    assert (tmp_dir / stage.relpath).parse() == {
        "md5": "a8157fb5b28457e1910506e9d4d4fca3",
        "frozen": True,
        "deps": [{"db": {"file_format": "json", "model": "model"}}],
        "outs": [
            {
                "hash": "md5",
                "md5": "56226d443067b195c90b427db557e1f2",
                "path": "model.json",
                "size": 243,
            }
        ],
    }

    dvc.update(stage.addressing)
    assert (tmp_dir / stage.relpath).parse() == {
        "md5": "2f84287f109fc2b8755a9e64ad910b5d",
        "frozen": True,
        "deps": [{"db": {"file_format": "json", "model": "model"}}],
        "outs": [
            {
                "hash": "md5",
                "md5": "08d806ee7e1f309c4561750805f02276",
                "path": "model.json",
                "size": 290,
            }
        ],
    }

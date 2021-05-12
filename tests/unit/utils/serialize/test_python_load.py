from pathlib import Path

from dvc.utils.serialize import load_py

fixtures = Path(__file__).parent / "fixtures"


def mod_to_dict(mod):
    return {k: getattr(mod, k) for k in dir(mod) if not k.startswith("_")}


def test_simple():
    from .fixtures import simple

    assert load_py(fixtures / "simple.py") == mod_to_dict(simple)


def test_composite_data():
    from .fixtures import composite

    assert load_py(fixtures / "composite.py") == mod_to_dict(composite)


def test_classy():
    assert load_py(fixtures / "classy.py") == {
        "Featurize": {"max_features": 3000, "ngrams": 2},
        "Prepare": {
            "bag": {"apple", "mango", "orange"},
            "seed": 20170428,
            "shuffle_dataset": True,
            "split": 0.2,
        },
        "Train": {
            "min_split": 64,
            "n_est": 100,
            "optimizer": "Adam",
            "seed": 20170428,
            "data": {"key1": "value1", "key2": "value2"},
        },
    }

"""Tests for params templating feature using ${DEFAULT_PARAMS_FILE.<key>} syntax."""

import pytest

from dvc.parsing import (
    DEFAULT_PARAMS_FILE,
    PARAMS_NAMESPACE,
    DataResolver,
    ResolveError,
)
from dvc.parsing.context import recurse_not_a_node


def assert_stage_equal(d1, d2):
    """Ensures both dictionaries don't contain Node objects and are equal."""
    for d in [d1, d2]:
        assert recurse_not_a_node(d)
    assert d1 == d2


class TestBasicParamsTemplating:
    """Test basic params templating functionality."""

    def test_simple_params_interpolation(self, tmp_dir, dvc):
        """Test basic ${PARAMS_NAMESPACE.key} interpolation in cmd."""
        params_data = {"learning_rate": 0.001, "epochs": 100}
        (tmp_dir / DEFAULT_PARAMS_FILE).dump(params_data)

        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": "python train.py "
                    f"--lr ${{{PARAMS_NAMESPACE}.learning_rate}}",
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                "train": {
                    "cmd": "python train.py --lr 0.001",
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }
        assert_stage_equal(result, expected)

    def test_params_in_outs(self, tmp_dir, dvc):
        """Test ${PARAMS_NAMESPACE.key} interpolation in outs field."""
        params_data = {"model_path": "models/model.pkl", "version": "v1"}
        (tmp_dir / DEFAULT_PARAMS_FILE).dump(params_data)

        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": "python train.py",
                    "outs": [f"${{{PARAMS_NAMESPACE}.model_path}}"],
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                "train": {
                    "cmd": "python train.py",
                    "outs": ["models/model.pkl"],
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }
        assert_stage_equal(result, expected)

    def test_params_nested_dict_access(self, tmp_dir, dvc):
        """Test ${PARAMS_NAMESPACE.nested.key} interpolation for nested dicts."""
        params_data = {
            "model": {"name": "resnet", "layers": 50},
            "training": {"lr": 0.001, "optimizer": "adam"},
        }
        (tmp_dir / DEFAULT_PARAMS_FILE).dump(params_data)

        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": "python train.py "
                    f"--model ${{{PARAMS_NAMESPACE}.model.name}} "
                    f"--lr ${{{PARAMS_NAMESPACE}.training.lr}}",
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                "train": {
                    "cmd": "python train.py --model resnet --lr 0.001",
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }
        assert_stage_equal(result, expected)

    def test_params_list_access(self, tmp_dir, dvc):
        """Test ${PARAMS_NAMESPACE.list[0]} interpolation for list access."""
        params_data = {
            "layers": [128, 256, 512],
            "models": ["model1", "model2"],
        }
        (tmp_dir / DEFAULT_PARAMS_FILE).dump(params_data)

        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": "python train.py "
                    f"--hidden ${{{PARAMS_NAMESPACE}.layers[0]}}",
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                "train": {
                    "cmd": "python train.py --hidden 128",
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }
        assert_stage_equal(result, expected)


class TestGlobalAndStageParams:
    """Test global-level and stage-level params."""

    def test_global_params(self, tmp_dir, dvc):
        """Test params defined at global level."""
        params_data = {"learning_rate": 0.001, "batch_size": 32}
        (tmp_dir / DEFAULT_PARAMS_FILE).dump(params_data)

        dvc_yaml = {
            "params": [DEFAULT_PARAMS_FILE],
            "stages": {
                "train": {
                    "cmd": "python train.py "
                    f"--lr ${{{PARAMS_NAMESPACE}.learning_rate}}",
                }
            },
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                "train": {
                    "cmd": "python train.py --lr 0.001",
                }
            }
        }
        assert_stage_equal(result, expected)

    def test_stage_params_override_global(self, tmp_dir, dvc):
        """Test that stage-level params are merged with global params."""
        (tmp_dir / DEFAULT_PARAMS_FILE).dump({"lr": 0.001})
        (tmp_dir / "config.yaml").dump({"batch_size": 64})

        dvc_yaml = {
            "params": [DEFAULT_PARAMS_FILE],
            "stages": {
                "train": {
                    "cmd": f"python train.py --lr ${{{PARAMS_NAMESPACE}.lr}} "
                    f"--bs ${{{PARAMS_NAMESPACE}.batch_size}}",
                    "params": [{"config.yaml": []}],
                }
            },
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                "train": {
                    "cmd": "python train.py --lr 0.001 --bs 64",
                    "params": [{"config.yaml": []}],
                }
            }
        }
        assert_stage_equal(result, expected)

    def test_multiple_param_files(self, tmp_dir, dvc):
        """Test loading params from multiple files."""
        (tmp_dir / "params1.yaml").dump({"lr": 0.001})
        (tmp_dir / "params2.yaml").dump({"epochs": 100})

        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": f"python train.py --lr ${{{PARAMS_NAMESPACE}.lr}} "
                    f"--epochs ${{{PARAMS_NAMESPACE}.epochs}}",
                    "params": [{"params1.yaml": []}, {"params2.yaml": []}],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                "train": {
                    "cmd": "python train.py --lr 0.001 --epochs 100",
                    "params": [{"params1.yaml": []}, {"params2.yaml": []}],
                }
            }
        }
        assert_stage_equal(result, expected)

    def test_specific_param_keys(self, tmp_dir, dvc):
        """Test loading specific keys from param files."""
        (tmp_dir / DEFAULT_PARAMS_FILE).dump(
            {
                "lr": 0.001,
                "epochs": 100,
                "batch_size": 32,
            }
        )

        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": f"python train.py --lr ${{{PARAMS_NAMESPACE}.lr}}",
                    "params": [{DEFAULT_PARAMS_FILE: ["lr", "epochs"]}],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                "train": {
                    "cmd": "python train.py --lr 0.001",
                    "params": [{DEFAULT_PARAMS_FILE: ["lr", "epochs"]}],
                }
            }
        }
        assert_stage_equal(result, expected)


class TestParamsNoConflictWithVars:
    """Test that params namespace doesn't conflict with vars."""

    def test_same_key_in_vars_and_params(self, tmp_dir, dvc):
        """
        Test that ${key} uses vars and ${PARAMS_NAMESPACE.key} uses params.
        """
        # Use a different file for params to avoid auto-loading conflict
        (tmp_dir / "model_params.yaml").dump({"model": "from_params"})

        dvc_yaml = {
            "vars": [{"model": "from_vars"}],
            "stages": {
                "train": {
                    "cmd": "python train.py "
                    f"--v ${{model}} --p ${{{PARAMS_NAMESPACE}.model}}",
                    "params": [{"model_params.yaml": []}],
                }
            },
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                "train": {
                    "cmd": "python train.py --v from_vars --p from_params",
                    "params": [{"model_params.yaml": []}],
                }
            }
        }
        assert_stage_equal(result, expected)


class TestParamsAmbiguityDetection:
    """Test detection of ambiguous param keys from multiple files."""

    def test_ambiguous_key_error(self, tmp_dir, dvc):
        """Test error when same key exists in multiple param files."""
        (tmp_dir / "params1.yaml").dump({"lr": 0.001})
        (tmp_dir / "params2.yaml").dump({"lr": 0.01})

        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": f"python train.py --lr ${{{PARAMS_NAMESPACE}.lr}}",
                    "params": [{"params1.yaml": []}, {"params2.yaml": []}],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)

        with pytest.raises(ResolveError, match=r"[Aa]mbiguous.*lr"):
            resolver.resolve()

    def test_no_ambiguity_with_nested_keys(self, tmp_dir, dvc):
        """Test no ambiguity when keys are nested differently."""
        (tmp_dir / "params1.yaml").dump({"model": {"lr": 0.001}})
        (tmp_dir / "params2.yaml").dump({"training": {"lr": 0.01}})

        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": f"python train.py --lr ${{{PARAMS_NAMESPACE}.model.lr}}",
                    "params": [{"params1.yaml": []}, {"params2.yaml": []}],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                "train": {
                    "cmd": "python train.py --lr 0.001",
                    "params": [{"params1.yaml": []}, {"params2.yaml": []}],
                }
            }
        }
        assert_stage_equal(result, expected)


class TestParamsFieldRestrictions:
    """Test that params interpolation is restricted to cmd and outs."""

    def test_params_not_allowed_in_deps(self, tmp_dir, dvc):
        """Test that ${PARAMS_NAMESPACE.*} is not allowed in deps field."""
        (tmp_dir / DEFAULT_PARAMS_FILE).dump({"data_path": "data.csv"})

        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": "python train.py",
                    "deps": [f"${{{PARAMS_NAMESPACE}.data_path}}"],
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)

        with pytest.raises(
            ResolveError, match=rf"{PARAMS_NAMESPACE}.*not allowed.*deps"
        ):
            resolver.resolve()

    def test_params_not_allowed_in_metrics(self, tmp_dir, dvc):
        """Test that ${PARAMS_NAMESPACE.*} is not allowed in metrics field."""
        (tmp_dir / DEFAULT_PARAMS_FILE).dump({"metrics_path": "metrics.json"})

        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": "python train.py",
                    "metrics": [f"${{{PARAMS_NAMESPACE}.metrics_path}}"],
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)

        with pytest.raises(
            ResolveError,
            match=rf"{PARAMS_NAMESPACE}.*not allowed.*metrics",
        ):
            resolver.resolve()


class TestParamsWithWdir:
    """Test params templating with working directory."""

    def test_params_with_wdir(self, tmp_dir, dvc):
        """Test that params are loaded from correct wdir."""
        data_dir = tmp_dir / "data"
        data_dir.mkdir()
        (data_dir / DEFAULT_PARAMS_FILE).dump({"lr": 0.001})

        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": f"python train.py --lr ${{{PARAMS_NAMESPACE}.lr}}",
                    "wdir": "data",
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                "train": {
                    "cmd": "python train.py --lr 0.001",
                    "wdir": "data",
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }
        assert_stage_equal(result, expected)


class TestParamsWithForeach:
    """Test params templating with foreach stages."""

    def test_params_in_foreach(self, tmp_dir, dvc):
        """Test ${PARAMS_NAMESPACE.*} works with foreach stages."""
        (tmp_dir / DEFAULT_PARAMS_FILE).dump({"base_lr": 0.001})

        dvc_yaml = {
            "stages": {
                "train": {
                    "foreach": [1, 2, 3],
                    "do": {
                        "cmd": "python train.py --seed ${item} "
                        f"--lr ${{{PARAMS_NAMESPACE}.base_lr}}",
                    },
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                "train@1": {"cmd": "python train.py --seed 1 --lr 0.001"},
                "train@2": {"cmd": "python train.py --seed 2 --lr 0.001"},
                "train@3": {"cmd": "python train.py --seed 3 --lr 0.001"},
            }
        }
        assert_stage_equal(result, expected)


class TestParamsErrorHandling:
    """Test error handling for params templating."""

    def test_missing_params_key(self, tmp_dir, dvc):
        """Test error when referenced param key doesn't exist."""
        (tmp_dir / DEFAULT_PARAMS_FILE).dump({"lr": 0.001})

        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": f"python train.py --epochs ${{{PARAMS_NAMESPACE}.epochs}}",
                    "params": [DEFAULT_PARAMS_FILE],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)

        with pytest.raises(
            ResolveError,
            match=rf"Could not find.*{PARAMS_NAMESPACE}\.epochs",
        ):
            resolver.resolve()

    def test_params_without_params_field(self, tmp_dir, dvc):
        """Test error when using ${PARAMS_NAMESPACE.*} without params field."""
        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": f"python train.py --lr ${{{PARAMS_NAMESPACE}.lr}}",
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)

        with pytest.raises(
            ResolveError, match=rf"Could not find.*{PARAMS_NAMESPACE}\.lr"
        ):
            resolver.resolve()

    def test_missing_params_file(self, tmp_dir, dvc):
        """Test error when param file doesn't exist."""
        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": f"python train.py --lr ${{{PARAMS_NAMESPACE}.lr}}",
                    "params": [{"nonexistent.yaml": []}],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)

        with pytest.raises(ResolveError, match="does not exist"):
            resolver.resolve()

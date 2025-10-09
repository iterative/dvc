"""
Tests for params templating feature using ${DEFAULT_PARAMS_FILE.<key>} syntax.
"""

import random
from typing import Optional

import pytest

from dvc.parsing import (
    DEFAULT_PARAMS_FILE,
    PARAMS_NAMESPACE,
    DataResolver,
    ResolveError,
)
from dvc.parsing.context import recurse_not_a_node

random.seed(17)


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
                    "params": [{DEFAULT_PARAMS_FILE: []}],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        with pytest.raises(
            ResolveError, match="interpolation is not allowed in 'outs'"
        ):
            resolver.resolve()

    def test_params_nested_dict_access(self, tmp_dir, dvc):
        """
        Test ${PARAMS_NAMESPACE.nested.key} interpolation for nested dicts.
        """
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
        foreach = list(range(1, 4))
        (tmp_dir / DEFAULT_PARAMS_FILE).dump({"base_lr": 0.001})

        dvc_yaml = {
            "stages": {
                "train": {
                    "foreach": foreach,
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
                f"train@{i}": {"cmd": f"python train.py --seed {i} --lr 0.001"}
                for i in foreach
            }
        }
        assert_stage_equal(result, expected)

    @pytest.mark.parametrize("explicit_keys", [True, False])
    def test_dynamic_params_file_loading_with_item(
        self, tmp_dir, dvc, explicit_keys: bool
    ):
        """Test dynamically loading param files based on ${item.*} values."""
        model_types = {"cnn": "images", "transformer": "text", "rnn": "sequences"}
        model_params = {
            model: {
                "DATA_DIR": f"/data/{model_type}",
                "BATCH_SIZE": random.randint(1, 40),
                "EPOCHS": random.randint(1, 40),
                "UNUSED_KEY": "unused",
            }
            for model, model_type in model_types.items()
        }
        for model, params in model_params.items():
            (tmp_dir / f"{model}_params.toml").dump(params)

        # Use foreach with dict items to dynamically load param files
        if explicit_keys:
            param_keys = ["DATA_DIR", "BATCH_SIZE", "EPOCHS"]
        else:
            param_keys = []

        params = [{"${item.model_type}_params.toml": param_keys}]

        dvc_yaml = {
            "stages": {
                "train": {
                    "foreach": [
                        {"model_type": model_type, "data_type": data_type}
                        for model_type, data_type in model_types.items()
                    ],
                    "do": {
                        "cmd": "python train.py --model ${item.model_type} "
                        f"--data-dir ${{{PARAMS_NAMESPACE}.DATA_DIR}} "
                        f"--batch-size ${{{PARAMS_NAMESPACE}.BATCH_SIZE}} "
                        f"--epochs ${{{PARAMS_NAMESPACE}.EPOCHS}}",
                        "params": params,
                    },
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                f"train@{i}": {
                    "cmd": f"python train.py --model {model} "
                    f"--data-dir {params['DATA_DIR']} "
                    f"--batch-size {params['BATCH_SIZE']} "
                    f"--epochs {params['EPOCHS']}",
                    "params": [{f"{model}_params.toml": param_keys}],
                }
                for i, (model, params) in enumerate(model_params.items())
            }
        }

        assert_stage_equal(result, expected)

    def test_dynamic_params_with_matrix(self, tmp_dir, dvc):
        """Test dynamic params loading with matrix stages."""
        # Define matrix dimensions
        envs = ["dev", "prod"]
        regions = ["us", "eu"]

        # Create param files for all combinations
        for env in envs:
            for region in regions:
                (tmp_dir / f"{env}_{region}_params.yaml").dump(
                    {"API_URL": f"{env}.{region}.api.com"}
                )

        dvc_yaml = {
            "stages": {
                "deploy": {
                    "matrix": {
                        "env": envs,
                        "region": regions,
                    },
                    "cmd": f"deploy.sh ${{{PARAMS_NAMESPACE}.API_URL}}",
                    "params": [{"${item.env}_${item.region}_params.yaml": []}],
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                f"deploy@{env}-{region}": {
                    "cmd": f"deploy.sh {env}.{region}.api.com",
                    "params": [{f"{env}_{region}_params.yaml": []}],
                }
                for env in envs
                for region in regions
            }
        }
        assert_stage_equal(result, expected)

    def test_dynamic_params_multiple_files(self, tmp_dir, dvc):
        """Test loading multiple dynamic param files in one stage."""
        foreach = ["cnn", "rnn"]
        # Create base and model-specific config files
        base_yaml = {"BATCH_SIZE": random.randint(0, 32)}
        (tmp_dir / "base.yaml").dump(base_yaml)
        model_yamls = {}
        for item in foreach:
            model_yamls[item] = {"LAYERS": random.randint(0, 32)}
            (tmp_dir / f"{item}_model.yaml").dump(model_yamls[item])

        dvc_yaml = {
            "stages": {
                "train": {
                    "foreach": foreach,
                    "do": {
                        "cmd": "train.py "
                        f"--batch ${{{PARAMS_NAMESPACE}.BATCH_SIZE}} "
                        f"--layers ${{{PARAMS_NAMESPACE}.LAYERS}}",
                        "params": [
                            {"base.yaml": []},
                            {"${item}_model.yaml": []},  # noqa: RUF027
                        ],
                    },
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                f"train@{item}": {
                    "cmd": "train.py "
                    f"--batch {base_yaml['BATCH_SIZE']} "
                    f"--layers {model_yamls[item]['LAYERS']}",
                    "params": [{"base.yaml": []}, {f"{item}_model.yaml": []}],
                }
                for item in foreach
            }
        }
        assert_stage_equal(result, expected)

    @pytest.mark.parametrize("file_ext", ["yaml", "json", "toml", "py"])
    def test_dynamic_params_different_formats(self, tmp_dir, dvc, file_ext):
        """Test dynamic param loading with different file formats."""
        # Python files need raw code, other formats use dump()
        if file_ext == "py":
            tmp_dir.gen(
                f"config.{file_ext}",
                f'ENDPOINT = "api.{file_ext}.com"\n',
            )
        else:
            (tmp_dir / f"config.{file_ext}").dump({"ENDPOINT": f"api.{file_ext}.com"})

        dvc_yaml = {
            "stages": {
                "process": {
                    "foreach": [file_ext],
                    "do": {
                        "cmd": f"process.sh ${{{PARAMS_NAMESPACE}.ENDPOINT}}",
                        "params": [{"config.${item}": []}],
                    },
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                f"process@{file_ext}": {
                    "cmd": f"process.sh api.{file_ext}.com",
                    "params": [{f"config.{file_ext}": []}],
                }
            }
        }
        assert_stage_equal(result, expected)

    def test_dynamic_params_nested_item_keys(self, tmp_dir, dvc):
        """Test dynamic params with nested item keys."""
        items = {"ml": "neural_net", "analytics": "regression"}
        # Create param files
        for k, v in items.items():
            (tmp_dir / f"{k}_params.yaml").dump({"MODEL": v})

        dvc_yaml = {
            "stages": {
                "process": {
                    "foreach": [
                        {"config": {"type": k, "version": i}}
                        for i, k in enumerate(items.keys())
                    ],
                    "do": {
                        "cmd": f"run.py ${{{PARAMS_NAMESPACE}.MODEL}} "
                        f"v${{item.config.version}}",
                        "params": [{"${item.config.type}_params.yaml": []}],
                    },
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                f"process@{i}": {
                    "cmd": f"run.py {v} v{i}",
                    "params": [{f"{k}_params.yaml": []}],
                }
                for i, (k, v) in enumerate(items.items())
            }
        }
        assert_stage_equal(result, expected)

    def test_dynamic_params_nonexistent_file_error(self, tmp_dir, dvc):
        """Test error when dynamically referenced param file doesn't exist."""
        dvc_yaml = {
            "stages": {
                "train": {
                    "foreach": ["model_a"],
                    "do": {
                        "cmd": f"train.py ${{{PARAMS_NAMESPACE}.lr}}",
                        "params": [{"${item}_params.yaml": []}],
                    },
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)

        with pytest.raises(ResolveError, match="does not exist"):
            resolver.resolve()

    def test_dynamic_params_with_global_params(self, tmp_dir, dvc):
        """Test dynamic stage params combined with global params."""
        # Global params
        global_seed = random.randint(1, 40)
        (tmp_dir / DEFAULT_PARAMS_FILE).dump({"SEED": global_seed})

        # Stage-specific params
        foreach = {"fast": random.randint(1, 40), "slow": random.randint(1, 40)}
        for k, v in foreach.items():
            (tmp_dir / f"{k}_params.yaml").dump({"TIMEOUT": v})

        dvc_yaml = {
            "params": [DEFAULT_PARAMS_FILE],
            "stages": {
                "test": {
                    "foreach": list(foreach.keys()),
                    "do": {
                        "cmd": "test.py "
                        f"--seed ${{{PARAMS_NAMESPACE}.SEED}} "
                        f"--timeout ${{{PARAMS_NAMESPACE}.TIMEOUT}}",
                        "params": [{"${item}_params.yaml": []}],
                    },
                }
            },
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                f"test@{k}": {
                    "cmd": f"test.py --seed {global_seed} --timeout {v}",
                    "params": [{f"{k}_params.yaml": []}],
                }
                for k, v in foreach.items()
            }
        }
        assert_stage_equal(result, expected)

    @pytest.mark.parametrize(
        "process_type", ["foreach", "foreach_list", "matrix", None]
    )
    def test_dynamic_params_ambiguity_detection(
        self, tmp_dir, dvc, process_type: Optional[str]
    ):
        """Test ambiguity detection with dynamically loaded params."""
        # Create two param files with same key
        configs = ["a", "b"]
        for config in configs:
            (tmp_dir / f"config_{config}.yaml").dump({"API_KEY": f"key_from_{config}"})

        # Both files define API_KEY - should detect ambiguity
        cmd = f"process.py ${{{PARAMS_NAMESPACE}.API_KEY}}"
        params = [
            {"config_a.yaml": []},
            {"config_b.yaml": []},
        ]
        if process_type == "foreach_list":
            process = {
                "foreach": [{"files": [f"config_{c}.yaml" for c in configs]}],
                "do": {"cmd": cmd, "params": params},
            }
        elif process_type == "foreach":
            process = {"foreach": configs, "do": {"cmd": cmd, "params": params}}
        elif process_type == "matrix":
            process = {"matrix": {"file": configs}, "cmd": cmd, "params": params}
        elif process_type is None:
            process = {"cmd": cmd, "params": params}
        else:
            raise ValueError(f"Invalid process_type {process_type}")

        dvc_yaml = {"stages": {"process": process}}

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)

        with pytest.raises(
            ResolveError,
            match=r"Ambiguous.*API_KEY.*config_a.yaml.*config_b.yaml",
        ):
            resolver.resolve()


class TestParamsNamespaceInteractions:
    """Test how param namespace interacts with other namespaces."""

    def test_params_and_vars_same_key(self, tmp_dir, dvc):
        """Test params and vars can have same key in different namespaces."""
        # Use different keys to avoid conflict - vars and params are separate
        keys = {"VAR_KEY": "from_vars", "TIMEOUT": 100}
        param_keys = {"PARAM_KEY": "from_params", "RETRIES": 3}

        (tmp_dir / "vars.yaml").dump(keys)
        (tmp_dir / "custom_params.yaml").dump(param_keys)

        dvc_yaml = {
            "vars": ["vars.yaml"],
            "stages": {
                "deploy": {
                    "cmd": f"deploy.sh ${{VAR_KEY}} ${{{PARAMS_NAMESPACE}.PARAM_KEY}} "
                    f"${{TIMEOUT}} ${{{PARAMS_NAMESPACE}.RETRIES}}",
                    "params": [{"custom_params.yaml": []}],
                }
            },
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                "deploy": {
                    "cmd": f"deploy.sh {keys['VAR_KEY']} {param_keys['PARAM_KEY']} "
                    f"{keys['TIMEOUT']} {param_keys['RETRIES']}",
                    "params": [{"custom_params.yaml": []}],
                }
            }
        }
        assert_stage_equal(result, expected)

    def test_params_vars_and_item_together(self, tmp_dir, dvc):
        """Test params, vars, and item namespaces coexist."""
        var_data = {"BASE_PATH": "/data", "VERSION": "v1"}
        envs = {"dev": "dev.api.com", "prod": "prod.api.com"}

        (tmp_dir / "vars.yaml").dump(var_data)
        for env, endpoint in envs.items():
            (tmp_dir / f"{env}_params.yaml").dump({"ENDPOINT": endpoint})

        dvc_yaml = {
            "vars": ["vars.yaml"],
            "stages": {
                "deploy": {
                    "foreach": list(envs.keys()),
                    "do": {
                        "cmd": f"deploy.sh --env ${{item}} "
                        f"--path ${{BASE_PATH}} "
                        f"--version ${{VERSION}} "
                        f"--endpoint ${{{PARAMS_NAMESPACE}.ENDPOINT}}",
                        "params": [{"${item}_params.yaml": []}],
                    },
                }
            },
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                f"deploy@{env}": {
                    "cmd": f"deploy.sh --env {env} "
                    f"--path {var_data['BASE_PATH']} "
                    f"--version {var_data['VERSION']} "
                    f"--endpoint {endpoint}",
                    "params": [{f"{env}_params.yaml": []}],
                }
                for env, endpoint in envs.items()
            }
        }
        assert_stage_equal(result, expected)

    def test_params_with_key_and_item_namespaces(self, tmp_dir, dvc):
        """Test params with foreach dict using ${key} and ${item}."""
        models = {f"model_{i}": {"lr": random.random()} for i in range(5)}
        params_data = {
            model: {"DATA_DIR": f"/data/{model}", "EPOCHS": random.randint(1, 10)}
            for model in models
        }

        for model, data in params_data.items():
            (tmp_dir / f"{model}_params.yaml").dump(data)

        dvc_yaml = {
            "stages": {
                "train": {
                    "foreach": models,
                    "do": {
                        "cmd": f"train.sh --model ${{key}} "
                        f"--lr ${{item.lr}} "
                        f"--data ${{{PARAMS_NAMESPACE}.DATA_DIR}} "
                        f"--epochs ${{{PARAMS_NAMESPACE}.EPOCHS}}",
                        "params": [{"${key}_params.yaml": []}],
                    },
                }
            }
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        result = resolver.resolve()

        expected = {
            "stages": {
                f"train@{model}": {
                    "cmd": f"train.sh --model {model} "
                    f"--lr {models[model]['lr']} "
                    f"--data {params_data[model]['DATA_DIR']} "
                    f"--epochs {params_data[model]['EPOCHS']}",
                    "params": [{f"{model}_params.yaml": []}],
                }
                for model in models
            }
        }
        assert_stage_equal(result, expected)

    def test_all_namespaces_in_outs(self, tmp_dir, dvc):
        """Test all namespaces (vars, item, param) used together in outs."""
        var_data = {"BASE_DIR": "outputs"}
        models = ["resnet", "transformer"]
        params_data = {
            f"exp{i}": {"MODEL": model, "VERSION": f"v{i}"}
            for i, model in enumerate(models)
        }

        (tmp_dir / "vars.yaml").dump(var_data)
        for exp, data in params_data.items():
            (tmp_dir / f"{exp}_params.yaml").dump(data)

        dvc_yaml = {
            "vars": ["vars.yaml"],
            "stages": {
                "train": {
                    "foreach": list(params_data.keys()),
                    "do": {
                        "cmd": "train.sh ${item}",
                        "params": [{"${item}_params.yaml": []}],
                        "outs": [
                            f"${{BASE_DIR}}/${{item}}/"
                            f"${{{PARAMS_NAMESPACE}.MODEL}}_"
                            f"${{{PARAMS_NAMESPACE}.VERSION}}.pkl"
                        ],
                    },
                }
            },
        }

        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)
        with pytest.raises(
            ResolveError, match="interpolation is not allowed in 'outs'"
        ):
            resolver.resolve()

    def test_param_reserved_namespace_in_vars(self, tmp_dir, dvc):
        """Test that PARAMS_NAMESPACE is reserved and cannot be used in vars."""
        (tmp_dir / "vars.yaml").dump({PARAMS_NAMESPACE: {"nested": "value"}})

        dvc_yaml = {
            "vars": ["vars.yaml"],
            "stages": {"test": {"cmd": "echo test"}},
        }

        # Should raise error about PARAMS_NAMESPACE being reserved
        with pytest.raises(ResolveError, match="reserved"):
            DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)


class TestParamsErrorHandling:
    """Test error handling for params templating."""

    def test_missing_params_key(self, tmp_dir, dvc):
        """Test error when referenced param key doesn't exist."""
        (tmp_dir / DEFAULT_PARAMS_FILE).dump({"lr": 0.001})

        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": f"python train.py--epochs ${{{PARAMS_NAMESPACE}.epochs}}",
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

    def test_foreach_with_dynamic_params_and_output(self, tmp_dir, dvc):
        """Test foreach with dynamic param file loading and file output.

        This tests the exact production use case where:
        - dvc.yaml is inside data/ directory
        - foreach generates stages with different model/data types
        - params files are loaded dynamically based on ${item.*}
        - param values are used in cmd and output path
        - no params.yaml file exists
        """
        # Remove params.yaml if it exists
        if (tmp_dir / DEFAULT_PARAMS_FILE).exists():
            (tmp_dir / DEFAULT_PARAMS_FILE).unlink()

        # Define test data
        items = [
            {"model_type": "region", "data_type": "full"},
            {"model_type": "country", "data_type": "full"},
        ]

        # Create directory structure and param files dynamically
        (tmp_dir / "data").mkdir()
        for item in items:
            # wdir is ../, so files are relative to repo root
            level_dir = tmp_dir / f"{item['model_type']}_level"
            level_dir.mkdir()

            # Create data-version.toml with DATA_DIR
            (level_dir / "data-version.toml").dump(
                {"DATA_DIR": f"/datasets/{item['model_type']}_data"}
            )

        dvc_yaml = {
            "stages": {
                "test": {
                    "foreach": items,
                    "do": {
                        "params": [
                            {"${item.model_type}_level/data-version.toml": ["DATA_DIR"]}
                        ],
                        "wdir": "../",
                        "cmd": (
                            f"echo ${{{PARAMS_NAMESPACE}.DATA_DIR}} > "
                            f"data/${{item.model_type}}_level/${{item.data_type}}/output.txt"
                        ),
                    },
                }
            }
        }

        # DataResolver is run from data/ directory
        resolver = DataResolver(dvc, (tmp_dir / "data").fs_path, dvc_yaml)
        result = resolver.resolve()

        # Build expected result with comprehension
        # Note: foreach with list of dicts generates numeric indices (@0, @1, ...)
        expected = {
            "stages": {
                f"test@{idx}": {
                    "params": [
                        {f"{item['model_type']}_level/data-version.toml": ["DATA_DIR"]}
                    ],
                    "wdir": "../",
                    "cmd": (
                        f"echo /datasets/{item['model_type']}_data > "
                        f"data/{item['model_type']}_level/{item['data_type']}/output.txt"
                    ),
                }
                for idx, item in enumerate(items)
            }
        }

        assert_stage_equal(result, expected)

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

    def test_has_params_interpolation_with_dict_values(self, tmp_dir, dvc):
        """Test has_params_interpolation with dict containing param interpolation."""
        # Test with dict containing param interpolation
        dvc_yaml = {
            "stages": {
                "train": {
                    "cmd": f"echo ${{{PARAMS_NAMESPACE}.lr}}",
                    "outs": {"model.pkl": {"cache": f"${{{PARAMS_NAMESPACE}.cache}}"}},
                    "params": [{DEFAULT_PARAMS_FILE: []}],
                }
            }
        }

        (tmp_dir / DEFAULT_PARAMS_FILE).dump({"lr": 0.001, "cache": True})
        resolver = DataResolver(dvc, tmp_dir.fs_path, dvc_yaml)

        # Should raise error because params interpolation in outs (dict value)
        with pytest.raises(
            ResolveError, match="interpolation is not allowed in 'outs'"
        ):
            resolver.resolve()

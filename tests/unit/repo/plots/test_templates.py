import os

import pytest

from dvc.repo.plots.template import (
    LinearTemplate,
    PlotTemplates,
    ScatterTemplate,
    TemplateContentDoesNotMatch,
    TemplateNotFoundError,
)


def test_raise_on_no_template(tmp_dir, dvc):
    with pytest.raises(TemplateNotFoundError):
        dvc.plots.templates.load("non_existing_template.json")


@pytest.mark.parametrize(
    "template_path, target_name",
    [
        (os.path.join(".dvc", "plots", "template.json"), "template"),
        (os.path.join(".dvc", "plots", "template.json"), "template.json"),
        (
            os.path.join(".dvc", "plots", "subdir", "template.json"),
            os.path.join("subdir", "template.json"),
        ),
        (
            os.path.join(".dvc", "plots", "subdir", "template.json"),
            os.path.join("subdir", "template"),
        ),
        ("template.json", "template.json"),
    ],
)
def test_load_template(tmp_dir, dvc, template_path, target_name):
    os.makedirs(os.path.abspath(os.path.dirname(template_path)), exist_ok=True)
    with open(template_path, "w", encoding="utf-8") as fd:
        fd.write("template_content")

    assert dvc.plots.templates.load(target_name).content == "template_content"


def test_load_default_template(tmp_dir, dvc):
    assert dvc.plots.templates.load(None).content == LinearTemplate().content


@pytest.mark.parametrize("output", ("output", None))
@pytest.mark.parametrize(
    "targets,expected_templates",
    (
        ([None, PlotTemplates.TEMPLATES]),
        (["linear", "scatter"], [ScatterTemplate, LinearTemplate]),
    ),
)
def test_init(tmp_dir, dvc, output, targets, expected_templates):
    output = output or dvc.plots.templates.templates_dir
    dvc.plots.templates.init(output, targets)

    assert set(os.listdir(output)) == {
        cls.DEFAULT_NAME + ".json" for cls in expected_templates
    }


def test_raise_on_init_modified(tmp_dir, dvc):
    dvc.plots.templates.init(output=None, targets=["linear"])

    with open(
        tmp_dir / ".dvc" / "plots" / "linear.json", "a", encoding="utf-8"
    ) as fd:
        fd.write("modification")

    with pytest.raises(TemplateContentDoesNotMatch):
        dvc.plots.templates.init(output=None, targets=["linear"])

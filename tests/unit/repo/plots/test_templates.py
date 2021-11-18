import os

import pytest

from dvc.repo.plots.template import TemplateNotFoundError


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
    with open(
        os.path.join(dvc.plots.templates.templates_dir, "linear.json"),
        "r",
        encoding="utf-8",
    ) as fd:
        content = fd.read()

    assert dvc.plots.templates.load(None).content == content

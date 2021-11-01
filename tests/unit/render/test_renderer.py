from dvc.render.base import Renderer


def test_remove_special_characters():
    special_chars = r"!@#$%^&*()[]{};,<>?\/:.|`~=_+"
    dirty = f"plot_name{special_chars}"
    assert Renderer._remove_special_chars(dirty) == "plot_name" + "_" * len(
        special_chars
    )

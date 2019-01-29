import yaml
from mock import MagicMock


def spy(method_to_decorate):
    mock = MagicMock()

    def wrapper(self, *args, **kwargs):
        mock(*args, **kwargs)
        return method_to_decorate(self, *args, **kwargs)

    wrapper.mock = mock
    return wrapper


def load_stage_file(path):
    with open(path, "r") as fobj:
        return yaml.safe_load(fobj)

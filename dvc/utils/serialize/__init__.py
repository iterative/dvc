from collections import defaultdict

from ._common import *  # noqa, pylint: disable=wildcard-import
from ._toml import *  # noqa, pylint: disable=wildcard-import
from ._yaml import *  # noqa, pylint: disable=wildcard-import

PARSERS = defaultdict(lambda: parse_yaml)  # noqa: F405
PARSERS.update({".toml": parse_toml})  # noqa: F405

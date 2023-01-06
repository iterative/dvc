import json

import pytest

pytestmark = pytest.mark.xfail(
    raises=(
        # looks like dvc-task is not saving json atomically
        json.JSONDecodeError,
        # needs investigation, maybe a file locking issue?
        PermissionError,
    ),
    strict=False,
    reason="See https://github.com/iterative/dvc/issues/8570",
)

from urllib.parse import urljoin

import pytest
from requests import Response

from dvc.utils.studio import STUDIO_URL, notify_refs


@pytest.mark.parametrize(
    "status_code, side_effect",
    [
        (200, {}),  # success
        (401, {"detail": "unauthorized"}),  # should not fail on client errors
        (500, ValueError),  # should not fail even on server errors
    ],
)
def test_notify_refs(mocker, status_code, side_effect):
    response = Response()
    response.status_code = status_code
    mocker.patch.object(response, "json", side_effect=[side_effect])

    mock_post = mocker.patch("requests.Session.post", return_value=response)

    notify_refs(
        "git@github.com:iterative/dvc.git",
        "TOKEN",
        pushed=["p1", "p2"],
        removed=["r1", "r2"],
    )

    assert mock_post.called
    assert mock_post.call_args == mocker.call(
        urljoin(STUDIO_URL, "/webhook/dvc"),
        json={
            "repo_url": "git@github.com:iterative/dvc.git",
            "client": "dvc",
            "refs": {
                "pushed": ["p1", "p2"],
                "removed": ["r1", "r2"],
            },
        },
        headers={"Authorization": "token TOKEN"},
        timeout=5,
        allow_redirects=False,
    )

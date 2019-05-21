from dvc.remote.gdrive.utils import (
    response_error_message,
    response_is_ratelimit,
)

from tests.unit.remote.gdrive.conftest import Response


def test_response_is_ratelimit(gdrive):
    assert response_is_ratelimit(
        Response({"error": {"errors": [{"domain": "usageLimits"}]}}, 403)
    )
    assert not response_is_ratelimit(Response(""))


def test_response_error_message(gdrive):
    r = Response({"error": {"message": "test"}})
    assert response_error_message(r) == "HTTP 200: test"
    r = Response("test")
    assert response_error_message(r) == "HTTP 200: test"

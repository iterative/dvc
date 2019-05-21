from tests.unit.remote.gdrive.conftest import AUTHORIZATION


def test_get_session(gdrive, no_requests):
    session = gdrive.client.oauth2.get_session()
    session.get("https://googleapis.com")
    args, kwargs = no_requests.call_args
    assert kwargs["headers"]["authorization"] == AUTHORIZATION["authorization"]

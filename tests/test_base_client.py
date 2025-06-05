import pytest

from marketpipe.ingestion.connectors.base_api_client import BaseApiClient
from marketpipe.ingestion.connectors.auth import AuthStrategy
from marketpipe.ingestion.connectors.models import ClientConfig


class DummyAuth(AuthStrategy):
    def apply(self, headers: dict, params: dict) -> None:
        pass


def test_abstract_enforcement():
    class IncompleteClient(BaseApiClient):
        def build_request_params(self, symbol: str, start_ts: int, end_ts: int, cursor=None):
            return {}

        def endpoint_path(self) -> str:
            return "/foo"

        def next_cursor(self, raw_json):
            return None

    with pytest.raises(TypeError):
        IncompleteClient(config=ClientConfig(api_key="k", base_url="http://x"), auth=DummyAuth())


def test_config_validation():
    with pytest.raises(Exception):
        ClientConfig(api_key=123, base_url=None)  # type: ignore


def test_pagination_iterator():
    pages = [{"cursor": "a"}, {"cursor": "b"}, {"cursor": None}]

    class PagingClient(BaseApiClient):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.calls = 0

        def build_request_params(self, symbol: str, start_ts: int, end_ts: int, cursor=None):
            return {}

        def endpoint_path(self) -> str:
            return "/test"

        def next_cursor(self, raw_json):
            return raw_json["cursor"]

        def parse_response(self, raw_json):
            return []

        def should_retry(self, status_code, json_body):
            return False

        def _request(self, params):
            result = pages[self.calls]
            self.calls += 1
            return result

    client = PagingClient(config=ClientConfig(api_key="t", base_url="http://x"), auth=DummyAuth())
    out = list(client.paginate("A", 1, 2))
    assert len(out) == 3


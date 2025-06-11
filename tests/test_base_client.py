# SPDX-License-Identifier: Apache-2.0
"""Legacy base client tests - maintaining backward compatibility."""

import pytest

from marketpipe.ingestion.infrastructure.base_api_client import BaseApiClient
from marketpipe.ingestion.infrastructure.auth import AuthStrategy
from marketpipe.ingestion.infrastructure.models import ClientConfig


class DummyMarketDataAuth(AuthStrategy):
    """Dummy authentication strategy for testing."""

    def apply(self, headers: dict, params: dict) -> None:
        pass


def test_legacy_abstract_base_client_enforces_complete_implementation():
    """Test that legacy abstract base client enforces complete implementation."""

    class IncompleteMarketDataClient(BaseApiClient):
        def build_request_params(
            self, symbol: str, start_ts: int, end_ts: int, cursor=None
        ):
            return {}

        def endpoint_path(self) -> str:
            return "/market-data"

        def next_cursor(self, raw_json):
            return None

    with pytest.raises(TypeError):
        IncompleteMarketDataClient(
            config=ClientConfig(api_key="k", base_url="http://x"),
            auth=DummyMarketDataAuth(),
        )


def test_legacy_client_config_validates_required_parameters():
    """Test that legacy client config validates required parameters."""
    with pytest.raises(Exception):
        ClientConfig(api_key=123, base_url=None)  # type: ignore


def test_legacy_base_client_supports_symbol_data_pagination():
    pages = [{"cursor": "a"}, {"cursor": "b"}, {"cursor": None}]

    class PagingClient(BaseApiClient):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.calls = 0

        def build_request_params(
            self, symbol: str, start_ts: int, end_ts: int, cursor=None
        ):
            return {}

        def endpoint_path(self) -> str:
            return "/symbol-data"

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

    client = PagingClient(
        config=ClientConfig(api_key="t", base_url="http://x"),
        auth=DummyMarketDataAuth(),
    )
    pagination_results = list(client.paginate("AAPL", 1, 2))
    assert len(pagination_results) == 3

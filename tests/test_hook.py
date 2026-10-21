"""Tests unitaires du MarketplaceAPIHook (requests mocké, aucune API réelle)."""

from unittest import mock

import pytest
import requests

from marketplace.hooks import MarketplaceAPIError, MarketplaceAPIHook


def _conn(host="http://api-marketplace:5000", password="secret-token"):
    conn = mock.MagicMock()
    conn.host, conn.password, conn.port, conn.schema = host, password, None, None
    return conn


def _response(status=200, payload=None):
    resp = mock.MagicMock(status_code=status, text=str(payload))
    resp.json.return_value = payload
    return resp


@pytest.fixture
def hook():
    h = MarketplaceAPIHook(backoff_factor=0)
    with mock.patch.object(MarketplaceAPIHook, "get_connection", return_value=_conn()):
        yield h


def test_bearer_token_comes_from_connection_password(hook):
    session = hook.get_conn()
    assert session.headers["Authorization"] == "Bearer secret-token"
    assert hook.base_url == "http://api-marketplace:5000"


def test_get_orders_calls_endpoint_with_date(hook):
    payload = {"date": "2026-04-07", "count": 1, "orders": [{"order_id": "ORD-1"}]}
    with mock.patch.object(requests.Session, "get", return_value=_response(200, payload)) as get:
        orders = hook.get_orders("2026-04-07")
    assert orders == [{"order_id": "ORD-1"}]
    url = get.call_args.args[0]
    assert url == "http://api-marketplace:5000/orders"
    assert get.call_args.kwargs["params"] == {"date": "2026-04-07"}


def test_retry_on_503_then_success(hook):
    responses = [_response(503, {}), _response(503, {}), _response(200, {"sellers": [{"seller_id": 1}]})]
    with mock.patch.object(requests.Session, "get", side_effect=responses) as get:
        assert hook.get_sellers() == [{"seller_id": 1}]
    assert get.call_count == 3


def test_retry_on_timeout_then_raise(hook):
    with mock.patch.object(requests.Session, "get", side_effect=requests.Timeout("boom")) as get:
        with pytest.raises(MarketplaceAPIError, match="4 tentatives"):
            hook.get_products()
    assert get.call_count == hook.max_retries + 1


def test_no_retry_on_401(hook):
    with mock.patch.object(requests.Session, "get", return_value=_response(401, {"error": "Unauthorized"})) as get:
        with pytest.raises(MarketplaceAPIError, match="401"):
            hook.get_orders("2026-04-07")
    assert get.call_count == 1


def test_connection_without_token_is_rejected():
    h = MarketplaceAPIHook()
    with mock.patch.object(MarketplaceAPIHook, "get_connection", return_value=_conn(password=None)):
        with pytest.raises(MarketplaceAPIError, match="token"):
            h.get_conn()

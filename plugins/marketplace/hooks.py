"""MarketplaceAPIHook — accès à l'API REST authentifiée du backoffice Maelys.

La Connection Airflow `marketplace_api` fournit :
    host     = URL de l'API (ex. http://api-marketplace:5000)
    password = token Bearer
Le token n'est jamais écrit dans le code.
"""

from __future__ import annotations

import time
from datetime import date

import requests

try:
    from airflow.sdk import BaseHook
except ImportError:  # Airflow 2.x
    from airflow.hooks.base import BaseHook

from airflow.exceptions import AirflowException

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class MarketplaceAPIError(AirflowException):
    """Erreur non récupérable renvoyée par l'API (4xx, payload invalide…)."""


class MarketplaceAPIHook(BaseHook):
    conn_name_attr = "marketplace_conn_id"
    default_conn_name = "marketplace_api"
    conn_type = "http"
    hook_name = "Marketplace API"

    def __init__(
        self,
        marketplace_conn_id: str = default_conn_name,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
    ) -> None:
        super().__init__()
        self.marketplace_conn_id = marketplace_conn_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self._session: requests.Session | None = None
        self._base_url: str | None = None

    # ------------------------------------------------------------------ conn
    def get_conn(self) -> requests.Session:
        if self._session is None:
            conn = self.get_connection(self.marketplace_conn_id)
            if not conn.host:
                raise MarketplaceAPIError(f"Connection '{self.marketplace_conn_id}' sans host")
            if not conn.password:
                raise MarketplaceAPIError(f"Connection '{self.marketplace_conn_id}' sans token (password)")
            host = conn.host.rstrip("/")
            if not host.startswith(("http://", "https://")):
                host = f"{conn.schema or 'http'}://{host}"
            if conn.port and f":{conn.port}" not in host:
                host = f"{host}:{conn.port}"
            self._base_url = host
            session = requests.Session()
            session.headers.update({
                "Authorization": f"Bearer {conn.password}",
                "Accept": "application/json",
            })
            self._session = session
        return self._session

    @property
    def base_url(self) -> str:
        self.get_conn()
        return self._base_url

    # --------------------------------------------------------------- request
    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        """GET avec retry exponentiel sur timeout, erreur réseau, 429 et 5xx."""
        session = self.get_conn()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        last_error: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = session.get(url, params=params, timeout=self.timeout)
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = exc
                self.log.warning("GET %s — erreur réseau (%s), tentative %d/%d",
                                 endpoint, exc.__class__.__name__, attempt + 1, self.max_retries + 1)
            else:
                if response.status_code in RETRYABLE_STATUS:
                    last_error = MarketplaceAPIError(f"HTTP {response.status_code} sur {endpoint}")
                    self.log.warning("GET %s — HTTP %d, tentative %d/%d",
                                     endpoint, response.status_code, attempt + 1, self.max_retries + 1)
                elif response.status_code >= 400:
                    raise MarketplaceAPIError(
                        f"HTTP {response.status_code} sur {endpoint} : {response.text[:200]}")
                else:
                    try:
                        return response.json()
                    except ValueError as exc:
                        raise MarketplaceAPIError(f"Réponse non JSON sur {endpoint}") from exc

            if attempt < self.max_retries:
                time.sleep(self.backoff_factor * (2 ** attempt))

        raise MarketplaceAPIError(
            f"GET {endpoint} en échec après {self.max_retries + 1} tentatives : {last_error}")

    # ------------------------------------------------------------ endpoints
    def health(self) -> dict:
        return self._get("/health")

    def get_orders(self, day: str | date) -> list[dict]:
        day_str = day.isoformat() if isinstance(day, date) else str(day)
        payload = self._get("/orders", params={"date": day_str})
        orders = payload.get("orders")
        if not isinstance(orders, list):
            raise MarketplaceAPIError(f"Payload /orders invalide pour {day_str}")
        self.log.info("API : %d commandes pour %s", len(orders), day_str)
        return orders

    def get_sellers(self, limit: int | None = None) -> list[dict]:
        payload = self._get("/sellers", params={"limit": limit} if limit else None)
        return payload["sellers"]

    def get_products(self) -> list[dict]:
        return self._get("/products")["products"]

    def get_customers(self, limit: int = 100) -> list[dict]:
        return self._get("/customers", params={"limit": limit})["customers"]

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.health()
            return True, "API Marketplace joignable"
        except Exception as exc:  # pragma: no cover
            return False, str(exc)

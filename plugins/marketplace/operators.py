"""DataQualityOperator — contrôles qualité configurables sur une table PostgreSQL.

Règles disponibles :
    {"type": "not_empty"}                              → au moins une ligne
    {"type": "not_null", "columns": ["a", "b"]}        → aucune valeur NULL
    {"type": "no_future_date", "column": "order_ts"}   → aucune date dans le futur

L'opérateur renvoie (XCom) {"passed": bool, "results": [...]} pour permettre
un branching en aval. Avec fail_on_error=True il lève une exception.
"""

from __future__ import annotations

import re
from typing import Any, Sequence

try:
    from airflow.sdk import BaseOperator
except ImportError:  # Airflow 2.x
    from airflow.models import BaseOperator

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")
SUPPORTED_RULES = {"not_empty", "not_null", "no_future_date"}


def _check_identifier(name: str) -> str:
    if not _IDENTIFIER.match(name):
        raise ValueError(f"Identifiant SQL invalide : {name!r}")
    return name


class DataQualityOperator(BaseOperator):
    template_fields: Sequence[str] = ("table", "partition_filter")
    ui_color = "#f9e79f"

    def __init__(
        self,
        *,
        table: str,
        rules: list[dict[str, Any]],
        conn_id: str = "postgres_dwh",
        partition_filter: str | None = None,
        fail_on_error: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table = table
        self.rules = rules
        self.conn_id = conn_id
        self.partition_filter = partition_filter
        self.fail_on_error = fail_on_error
        for rule in rules:
            if rule.get("type") not in SUPPORTED_RULES:
                raise ValueError(f"Règle inconnue : {rule!r} (supportées : {sorted(SUPPORTED_RULES)})")

    def _where(self, extra: str | None = None) -> str:
        clauses = [c for c in (self.partition_filter, extra) if c]
        return f" WHERE {' AND '.join(f'({c})' for c in clauses)}" if clauses else ""

    def build_queries(self) -> list[tuple[dict, str, str]]:
        """(règle, libellé, SQL comptant les lignes en violation)."""
        table = _check_identifier(self.table)
        queries = []
        for rule in self.rules:
            kind = rule["type"]
            if kind == "not_empty":
                queries.append((rule, "not_empty", f"SELECT COUNT(*) FROM {table}{self._where()}"))
            elif kind == "not_null":
                for col in rule["columns"]:
                    col = _check_identifier(col)
                    queries.append((rule, f"not_null({col})",
                                    f"SELECT COUNT(*) FROM {table}{self._where(f'{col} IS NULL')}"))
            elif kind == "no_future_date":
                col = _check_identifier(rule["column"])
                queries.append((rule, f"no_future_date({col})",
                                f"SELECT COUNT(*) FROM {table}{self._where(f'{col} > now()')}"))
        return queries

    def execute(self, context) -> dict:
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        results = []
        for rule, label, sql in self.build_queries():
            count = int(hook.get_first(sql)[0])
            passed = count > 0 if rule["type"] == "not_empty" else count == 0
            detail = f"{count} ligne(s)" if rule["type"] == "not_empty" else f"{count} violation(s)"
            results.append({"rule": label, "passed": passed, "detail": detail})
            self.log.info("DQ %-28s %s  (%s)", label, "✅ OK" if passed else "❌ KO", detail)

        passed = all(r["passed"] for r in results)
        failures = [r for r in results if not r["passed"]]
        if failures:
            self.log.error("Data quality KO sur %s : %s", self.table, failures)
            if self.fail_on_error:
                raise AirflowException(f"Data quality KO sur {self.table} : {failures}")
        return {"passed": passed, "table": self.table, "results": results}

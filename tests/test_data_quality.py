"""Tests du DataQualityOperator avec une base mockée (aucun PostgreSQL requis)."""

from unittest import mock

import pytest

from marketplace.operators import DataQualityOperator

RULES = [
    {"type": "not_empty"},
    {"type": "not_null", "columns": ["order_id", "seller_id"]},
    {"type": "no_future_date", "column": "order_ts"},
]


def _operator(**kwargs):
    return DataQualityOperator(task_id="dq", table="staging.orders",
                               partition_filter="dt = '2026-04-07'", rules=RULES, **kwargs)


def _run(op, counts):
    """counts : résultats successifs de COUNT(*) renvoyés par la base mockée."""
    with mock.patch("marketplace.operators.PostgresHook") as hook_cls:
        hook_cls.return_value.get_first.side_effect = [(c,) for c in counts]
        result = op.execute(context={})
        return result, hook_cls.return_value.get_first.call_args_list


def test_all_rules_pass():
    result, calls = _run(_operator(), [7350, 0, 0, 0])
    assert result["passed"] is True
    assert len(calls) == 4
    assert all("dt = '2026-04-07'" in c.args[0] for c in calls)


def test_empty_partition_fails():
    result, _ = _run(_operator(), [0, 0, 0, 0])
    assert result["passed"] is False
    assert [r["rule"] for r in result["results"] if not r["passed"]] == ["not_empty"]


def test_null_and_future_dates_are_detected():
    result, calls = _run(_operator(), [100, 3, 0, 5])
    failed = {r["rule"] for r in result["results"] if not r["passed"]}
    assert failed == {"not_null(order_id)", "no_future_date(order_ts)"}
    assert "order_ts > now()" in calls[3].args[0]


def test_fail_on_error_raises():
    with pytest.raises(Exception, match="Data quality KO"):
        _run(_operator(fail_on_error=True), [0, 0, 0, 0])


def test_unknown_rule_is_rejected():
    with pytest.raises(ValueError, match="Règle inconnue"):
        DataQualityOperator(task_id="dq", table="t", rules=[{"type": "unique"}])


def test_sql_injection_in_identifier_is_rejected():
    op = DataQualityOperator(task_id="dq", table="staging.orders; DROP TABLE x",
                             rules=[{"type": "not_empty"}])
    with pytest.raises(ValueError, match="Identifiant SQL invalide"):
        op.build_queries()

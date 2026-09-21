from datetime import datetime, timezone
from decimal import Decimal
import random

import pytest

from data_generator import api_generator, local_generator, mysql_generator
from data_generator import postgres_generator, run_all_generator


def test_dependency_order_respects_every_parent():
    order = run_all_generator.dependency_order()
    positions = {name: index for index, name in enumerate(order)}
    assert set(order) == set(run_all_generator.DEPENDENCIES)
    for child, parents in run_all_generator.DEPENDENCIES.items():
        assert all(positions[parent] < positions[child] for parent in parents)


def test_dependency_order_detects_cycle(monkeypatch):
    monkeypatch.setattr(run_all_generator, "DEPENDENCIES", {"a": ("b",), "b": ("a",)})
    with pytest.raises(ValueError, match="ciclo"):
        run_all_generator.dependency_order()


def test_generation_is_reproducible_for_seed_and_run_id():
    first = run_all_generator.Generation(1000, 42, "same-run")
    second = run_all_generator.Generation(1000, 42, "same-run")
    assert first.run_id == second.run_id
    assert first.pg_counts == second.pg_counts
    assert first.mysql_count == second.mysql_count
    assert first.api_counts == second.api_counts
    assert first.local_counts == second.local_counts


def test_run_steps_requires_complete_mysql_transaction():
    with pytest.raises(ValueError, match="orders, order_items e inventory"):
        run_all_generator.run_steps(["orders"], dry_run=True)


def test_local_timestamp_requires_timezone():
    with pytest.raises(ValueError, match="fuso"):
        local_generator.stamp(datetime(2026, 1, 1))
    assert local_generator.stamp(datetime(2026, 1, 1, tzinfo=timezone.utc)).endswith("Z")


def test_next_id_ignores_empty_values():
    assert local_generator.next_id([{"id": ""}, {"id": "4"}], "id") == 5


def test_exchange_rate_is_deterministic_per_day():
    now = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
    assert api_generator.generate_exchange_rates(now) == api_generator.generate_exchange_rates(now)


def test_campaign_generator_respects_budget_and_dates():
    rows = api_generator.generate_marketing_campaigns(
        random.Random(1), 20, datetime(2026, 1, 10, tzinfo=timezone.utc)
    )
    assert len(rows) == 20
    assert all(row["actual_spend"] <= row["budget"] for row in rows)
    assert all(row["start_date"] <= row["end_date"] for row in rows)


def test_money_helpers_round_to_cents():
    assert postgres_generator.money("1.235") == Decimal("1.24")
    assert mysql_generator.money("1.235") == Decimal("1.24")


def test_products_require_suppliers():
    with pytest.raises(ValueError, match="Fornecedores"):
        postgres_generator.generate_products(random.Random(1), 1, "run", [])


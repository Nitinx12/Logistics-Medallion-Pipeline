"""Tests for SCD2 logic matching dbt drivers_snapshot / dim_driver."""

# ruff: noqa: DTZ001
from __future__ import annotations

import hashlib
from datetime import datetime

import pandas as pd


def make_surrogate_key(natural_key: str, valid_from: datetime) -> str:
    """Mirrors: md5(concat(driver_id, cast(valid_from as string)))"""
    return hashlib.md5(f"{natural_key}{valid_from}".encode()).hexdigest()


def test_surrogate_key_is_stable() -> None:
    sk1 = make_surrogate_key("D00001", datetime(2020, 1, 1))
    sk2 = make_surrogate_key("D00001", datetime(2020, 1, 1))
    assert sk1 == sk2


def test_surrogate_key_differs_per_version() -> None:
    sk1 = make_surrogate_key("D00001", datetime(2020, 1, 1))
    sk2 = make_surrogate_key("D00001", datetime(2021, 6, 1))
    assert sk1 != sk2


def test_surrogate_key_differs_per_natural_key() -> None:
    sk1 = make_surrogate_key("D00001", datetime(2020, 1, 1))
    sk2 = make_surrogate_key("D00002", datetime(2020, 1, 1))
    assert sk1 != sk2


def test_scd2_valid_to_is_next_valid_from() -> None:
    data = [
        {
            "driver_id": "D1",
            "dbt_valid_from": datetime(2020, 1, 1),
            "dbt_valid_to": datetime(2021, 1, 1),
        },
        {
            "driver_id": "D1",
            "dbt_valid_from": datetime(2021, 1, 1),
            "dbt_valid_to": None,
        },
    ]
    df = pd.DataFrame(data)
    closed = df[df["dbt_valid_to"].notna()].iloc[0]
    current = df[df["dbt_valid_to"].isna()].iloc[0]
    assert closed["dbt_valid_to"] == current["dbt_valid_from"]


def test_only_one_current_row_per_driver() -> None:
    data = [
        {"driver_id": "D1", "dbt_valid_to": datetime(2021, 1, 1)},
        {"driver_id": "D1", "dbt_valid_to": None},
    ]
    df = pd.DataFrame(data)
    df["is_current"] = df["dbt_valid_to"].isna()
    assert df[df["is_current"]].shape[0] == 1


def test_scd2_helper_valid_to_shift() -> None:
    df = pd.DataFrame(
        [
            {"id": "D1", "updated_at": "2020-01-01"},
            {"id": "D1", "updated_at": "2021-01-01"},
        ]
    )
    df = df.sort_values(["id", "updated_at"])
    df["valid_from"] = pd.to_datetime(df["updated_at"])
    df["valid_to"] = df.groupby("id")["valid_from"].shift(-1)
    df["is_current"] = df["valid_to"].isna()
    assert df.iloc[0]["valid_to"] == pd.Timestamp("2021-01-01")
    assert bool(df.iloc[1]["is_current"]) is True


def test_scd2_current_row_coalesce() -> None:

    df = pd.DataFrame(
        [
            {"driver_id": "D1", "valid_to": pd.Timestamp("2021-01-01")},
            {"driver_id": "D1", "valid_to": pd.NaT},
        ]
    )
    df["valid_to_coalesced"] = df["valid_to"].fillna(pd.Timestamp("9999-12-31"))
    assert df.iloc[1]["valid_to_coalesced"] == pd.Timestamp("9999-12-31")

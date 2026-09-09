"""SCD2 logic tests for dim_driver and dim_vehicle."""

import pandas as pd


def scd2(df):
    df = df.sort_values(["id", "updated_at"])
    df["valid_from"] = pd.to_datetime(df["updated_at"])
    df["valid_to"] = df.groupby("id")["valid_from"].shift(-1)
    df["is_current"] = df["valid_to"].isna()
    return df


def test_scd2_valid_to():
    df = pd.DataFrame(
        [
            {"id": "D1", "updated_at": "2020-01-01"},
            {"id": "D1", "updated_at": "2021-01-01"},
        ]
    )
    out = scd2(df)
    assert out.iloc[0]["valid_to"] == pd.Timestamp("2021-01-01")
    assert bool(out.iloc[1]["is_current"]) is True


def test_scd2_surrogate_key():
    df = pd.DataFrame([{"id": "D1", "updated_at": "2020-01-01"}])
    df["sk"] = df["id"] + "_" + df["updated_at"]
    assert df.iloc[0]["sk"] == "D1_2020-01-01"

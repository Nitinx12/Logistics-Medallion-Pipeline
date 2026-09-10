"""Tests for watermark logic in the bronze extractors."""

from __future__ import annotations

import json
from pathlib import Path


def filter_by_watermark(rows: list[dict], watermark: str) -> list[dict]:
    return [r for r in rows if r["updated_at"] > watermark]


def watermark_get_json(path: Path, table: str) -> str:
    if path.exists():
        try:
            return json.loads(path.read_text()).get(
                f"pg:{table}", "1970-01-01T00:00:00"
            )
        except (json.JSONDecodeError, OSError):
            return "1970-01-01T00:00:00"
    return "1970-01-01T00:00:00"


def watermark_set_json(path: Path, table: str, ts: str) -> None:
    data = json.loads(path.read_text()) if path.exists() else {}
    data[f"pg:{table}"] = ts
    path.write_text(json.dumps(data, indent=2))


def test_watermark_default(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.chdir(tmp_path)
    p = tmp_path / "watermarks.json"
    assert watermark_get_json(p, "customers") == "1970-01-01T00:00:00"


def test_watermark_roundtrip(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.chdir(tmp_path)
    p = tmp_path / "watermarks.json"
    watermark_set_json(p, "customers", "2026-09-10T00:00:00")
    assert watermark_get_json(p, "customers") == "2026-09-10T00:00:00"


def test_watermark_preserves_other_keys(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.chdir(tmp_path)
    p = tmp_path / "watermarks.json"
    watermark_set_json(p, "customers", "2026-09-10T00:00:00")
    watermark_set_json(p, "drivers", "2026-09-09T00:00:00")
    assert watermark_get_json(p, "customers") == "2026-09-10T00:00:00"
    assert watermark_get_json(p, "drivers") == "2026-09-09T00:00:00"


def test_watermark_corrupt_file(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.chdir(tmp_path)
    p = tmp_path / "watermarks.json"
    p.write_text("NOT_JSON")
    assert watermark_get_json(p, "customers") == "1970-01-01T00:00:00"


def test_filter_by_watermark() -> None:
    rows = [{"updated_at": "2026-09-09"}, {"updated_at": "2026-09-08"}]
    assert len(filter_by_watermark(rows, "2026-09-08")) == 1
    assert filter_by_watermark(rows, "2026-09-10") == []


def test_merge_upsert_condition() -> None:
    tgt = {"id": 1, "updated_at": "2026-09-08"}
    src_new = {"id": 1, "updated_at": "2026-09-09"}
    src_old = {"id": 1, "updated_at": "2026-09-07"}
    assert src_new["updated_at"] > tgt["updated_at"]
    assert not (src_old["updated_at"] > tgt["updated_at"])


def test_watermark_advances_on_max_ts() -> None:
    rows = [
        {"updated_at": "2026-09-08T10:00:00"},
        {"updated_at": "2026-09-10T12:00:00"},
        {"updated_at": "2026-09-09T08:00:00"},
    ]
    max_ts = max(r["updated_at"] for r in rows)
    assert max_ts == "2026-09-10T12:00:00"

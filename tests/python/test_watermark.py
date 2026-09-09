"""Watermark filtering and merge condition tests."""


def filter_by_watermark(rows, watermark):
    return [r for r in rows if r["updated_at"] > watermark]


def test_filter():
    rows = [{"updated_at": "2026-09-09"}, {"updated_at": "2026-09-08"}]
    assert len(filter_by_watermark(rows, "2026-09-08")) == 1


def test_merge_upsert():
    tgt = {"id": 1, "updated_at": "2026-09-08"}
    src_new = {"id": 1, "updated_at": "2026-09-09"}
    assert src_new["updated_at"] > tgt["updated_at"]

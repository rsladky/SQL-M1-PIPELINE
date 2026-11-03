"""All positions and identities are synthetic fixtures; no vessel data in Git."""

import json
from datetime import date, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from scripts.transform.aggregate import PUBLIC_SCHEMA, candidate
from scripts.transform.batches import SCHEMA, sha256, transform


def noaa_row(day, mmsi=100000001, *, kind=70, lat=40.5, lon=-73.5, hour=12, sog="102.3", cog="360.0", heading="511"):
    return {
        "source": "noaa",
        "source_url": f"https://noaaocm.blob.core.windows.net/ais/csv2/csv{day[:4]}/ais-{day}.csv.zst",
        "observed_at": f"{day}T{hour:02d}:00:00Z", "vessel_type": str(kind),
        "record": {"mmsi": str(mmsi), "latitude": str(lat), "longitude": str(lon),
                   "vessel_type": str(kind), "sog": sog, "cog": cog, "heading": heading,
                   "vessel_name": "SYNTHETIC", "imo": "0"},
    }


def noaa_input(root: Path, day: str, rows: list[dict]):
    base = root / "noaa"
    base.mkdir(parents=True, exist_ok=True)
    path = base / f"{day}-ny-nj.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    (base / f"{day}-ny-nj.manifest.json").write_text(json.dumps({
        "source": "noaa", "date": day, "zone": "ny-nj", "sha256": sha256(path),
        "output_bytes": path.stat().st_size,
        "url": f"https://noaaocm.blob.core.windows.net/ais/csv2/csv{day[:4]}/ais-{day}.csv.zst",
    }))
    return path


def ais_input(root: Path):
    base = root / "aisstream"
    base.mkdir(parents=True, exist_ok=True)
    path = base / "synthetic-session.jsonl"
    row = {"source": "aisstream", "received_at": "2024-01-14T12:00:00Z", "vessel_type": 71,
           "record": {"MessageType": "PositionReport", "MetaData": {
               "MMSI": 100000001, "Latitude": 40.1, "Longitude": -73.1},
               "Message": {"PositionReport": {"Sog": 10, "Cog": 20}}}}
    path.write_text(json.dumps(row) + "\n")
    path.with_suffix(".manifest.json").write_text(json.dumps({
        "source": "aisstream", "session_id": "synthetic-session", "rows": 1,
    }))
    return path


def test_filter_dedup_sentinels_partition_and_resume(tmp_path):
    raw, processed = tmp_path / "raw", tmp_path / "processed"
    day = "2024-01-14"
    good = noaa_row(day)
    rows = [good, good, noaa_row(day, kind=80), noaa_row(day, lat=91),
            noaa_row(day, mmsi=100000002, lat=40.6, lon=-73.2, hour=13)]
    meta = transform(noaa_input(raw, day, rows), root=processed, batch_size=2)
    assert meta["counts"] == {"input": 5, "cargo": 2, "noncargo": 1,
                              "invalid": 1, "duplicates": 1, "quality_flags": 0}
    path = processed / meta["files"][0]
    assert "source/noaa/date=2024-01-14" in str(path)
    parquet = pq.read_table(path)
    assert pq.ParquetFile(path).schema_arrow.equals(SCHEMA)
    assert parquet.column("date").to_pylist() == [day, day]
    assert parquet.num_rows == 2
    assert parquet.column("sog").to_pylist() == [None, None]
    assert parquet.column("cog").to_pylist() == [None, None]
    assert parquet.column("heading").to_pylist() == [None, None]
    assert transform(raw / "noaa" / f"{day}-ny-nj.jsonl", root=processed) == meta
    path.write_bytes(b"corrupt")
    rebuilt = transform(raw / "noaa" / f"{day}-ny-nj.jsonl", root=processed)
    assert rebuilt["counts"] == meta["counts"]
    assert rebuilt["files_sha256"] == meta["files_sha256"]  # deterministic rebuild
    assert pq.read_table(path).num_rows == 2


def test_sources_separated_and_quality_flags(tmp_path):
    raw, processed = tmp_path / "raw", tmp_path / "processed"
    day = "2024-01-14"
    rows = [noaa_row(day, hour=12), noaa_row(day, hour=13, lat=41.5),
            noaa_row(day, hour=14, lat=40.51), noaa_row(day, hour=22, lat=40.52)]
    noaa_meta = transform(noaa_input(raw, day, rows), root=processed)
    ais_meta = transform(ais_input(raw), root=processed)
    noaa_data = pq.read_table(processed / noaa_meta["files"][0]).to_pylist()
    ais_data = pq.read_table(processed / ais_meta["files"][0]).to_pylist()
    assert [row["quality_flag"] for row in noaa_data] == [
        "ok", "implausible_jump", "ok", "tracking_gap"]
    assert len(ais_data) == 1 and ais_data[0]["source"] == "aisstream"
    assert ais_data[0]["mmsi"] == noaa_data[0]["mmsi"]
    assert not set(noaa_meta["files"]) & set(ais_meta["files"])
    assert candidate("2024-01", raw_root=raw, processed_root=processed)["reason"] == "incomplete_month"
    assert not list((processed / "noaa_candidate").glob("*.parquet")) if (processed / "noaa_candidate").exists() else True


def test_aisstream_cross_midnight_partitions_and_invalid_timestamp(tmp_path):
    raw, processed = tmp_path / "raw", tmp_path / "processed"
    path = ais_input(raw)
    first = json.loads(path.read_text())
    first["received_at"] = "2024-01-14T23:59:00Z"
    second = json.loads(path.read_text())
    second["received_at"] = "2024-01-15T00:01:00Z"
    second["record"]["MetaData"]["Latitude"] = 40.1001
    bad = json.loads(path.read_text())
    bad["received_at"] = "2024-01-14T12:00:00"  # ambiguous local time
    path.write_text("".join(json.dumps(row) + "\n" for row in (first, second, bad)))
    meta = transform(path, root=processed, batch_size=1)
    assert meta["counts"]["invalid"] == 1
    assert [p.split("/")[2] for p in meta["files"]] == ["date=2024-01-14", "date=2024-01-15"]
    assert all(pq.read_table(processed / part).num_rows == 1 for part in meta["files"])
    assert candidate("2024-01", raw_root=raw, processed_root=processed)["reason"] == "incomplete_month"


def test_full_month_thresholds_and_no_sensitive_columns(tmp_path):
    raw, processed = tmp_path / "raw", tmp_path / "processed"
    first = date(2024, 1, 1)
    for offset in range(31):
        day = (first + timedelta(days=offset)).isoformat()
        rows = []
        if offset < 7:
            for vessel in range(20):
                rows.extend([noaa_row(day, mmsi=100000001 + vessel, hour=12),
                             noaa_row(day, mmsi=100000001 + vessel, hour=13, lat=40.5001)])
        transform(noaa_input(raw, day, rows), root=processed, batch_size=11)
    result = candidate("2024-01", raw_root=raw, processed_root=processed)
    assert result["status"] == "candidate_not_approved_for_publication"
    output = processed / "noaa_candidate/2024-01-ny-nj.parquet"
    table = pq.read_table(output)
    assert table.schema.equals(PUBLIC_SCHEMA)
    assert table.to_pylist() == [{"month": "2024-01", "region": "ny-nj",
                                  "cargo_vessels": 20, "observations": 280}]
    assert not any(s in output.read_bytes().lower() for s in (b"mmsi", b"imo", b"latitude", b"aisstream", b"shipname"))
    # If the complete-month evidence disappears, an old candidate is withdrawn.
    (raw / "noaa/2024-01-31-ny-nj.manifest.json").unlink()
    assert candidate("2024-01", raw_root=raw, processed_root=processed)["reason"] == "incomplete_month"
    assert not output.exists()


@pytest.mark.parametrize("vessels,observations_per_day,days,dominant", [
    (19, 2, 7, False),  # minimum distinct vessels
    (20, 1, 7, False),  # minimum observations
    (20, 2, 6, False),  # minimum observed days
    (20, 2, 7, True),   # one vessel dominates although all minimums pass
])
def test_complete_month_privacy_suppression(tmp_path, vessels, observations_per_day, days, dominant):
    raw, processed = tmp_path / "raw", tmp_path / "processed"
    for offset in range(31):
        day = (date(2024, 1, 1) + timedelta(days=offset)).isoformat()
        rows = []
        if offset < days:
            for vessel in range(vessels):
                for i in range(observations_per_day):
                    rows.append(noaa_row(day, mmsi=100000001 + vessel,
                                         hour=12 + i, lat=40.5 + i * 0.0001))
            if dominant:
                for minute in range(8):
                    row = noaa_row(day, mmsi=100000001, lat=40.5 + minute * 0.00001)
                    row["observed_at"] = f"{day}T12:{minute + 1:02d}:00Z"
                    rows.append(row)
        transform(noaa_input(raw, day, rows), root=processed)
    result = candidate("2024-01", raw_root=raw, processed_root=processed)
    assert result["reason"] == "privacy_thresholds"
    assert not (processed / "noaa_candidate/2024-01-ny-nj.parquet").exists()


def test_privacy_threshold_and_spoofed_source(tmp_path):
    raw, processed = tmp_path / "raw", tmp_path / "processed"
    day = "2024-01-14"
    spoof = noaa_row(day)
    spoof["source"] = "aisstream"
    meta = transform(noaa_input(raw, day, [spoof, noaa_row(day, kind=69)]), root=processed)
    assert meta["counts"]["invalid"] == 1
    assert meta["counts"]["noncargo"] == 1
    assert not meta["files"]
    with pytest.raises(ValueError, match="pre-approved"):
        candidate("2024-01", zone="secret-grid", raw_root=raw, processed_root=processed)

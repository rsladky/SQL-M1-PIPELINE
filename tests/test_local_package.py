"""Only synthetic records and temporary local directories; no Hub client/network."""

import json
import socket
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.ingest.noaa import PILOT_BOUNDS
from scripts.publish.local_package import package_local
from scripts.transform.aggregate import PUBLIC_SCHEMA
from scripts.transform.batches import sha256
from test_publish import _complete_month


def prepared_month(tmp_path):
    raw, processed = _complete_month(tmp_path)
    # The older synthetic fixture has no geo metadata; real NOAA manifests do.
    for manifest in (raw / "noaa").glob("*.manifest.json"):
        value = json.loads(manifest.read_text())
        value["bounds"] = list(PILOT_BOUNDS)
        manifest.write_text(json.dumps(value))
    return raw, processed


def test_complete_noaa_month_creates_local_draft_without_network(tmp_path, monkeypatch):
    raw, processed = prepared_month(tmp_path)
    candidate = processed / "noaa_candidate/2024-01-ny-nj.parquet"
    before = sha256(candidate)
    output = tmp_path / "data/local_dataset"

    def network_forbidden(*args, **kwargs):
        raise AssertionError("network must never be contacted")
    monkeypatch.setattr(socket.socket, "connect", network_forbidden)
    result = package_local("2024-01", raw_root=raw, processed_root=processed, output_root=output)
    directory = output / "noaa-2024-01-ny-nj"
    assert result["path"] == str(directory) and result["status"] == "draft_local_not_published"
    assert result["network_calls"] == 0 and result["rights"] == "not established for Hub hosting or redistribution"
    assert result["calendar_days_verified"] == 31 and result["observed_days"] == 7
    assert {p.name for p in directory.iterdir()} == {"README.md", "manifest.json", "data"}
    assert {p.name for p in (directory / "data").iterdir()} == {"noaa_aggregate_2024-01.parquet"}
    card = (directory / "README.md").read_text()
    assert "DRAFT LOCAL ONLY — NOT PUBLISHED" in card
    assert "NOAA Office for Coastal Management" in card
    assert "daily national CSV.zst" in card
    assert "not been established" in card
    assert "No CC0" in card
    assert "{{" not in card and "}}" not in card
    assert "100000001" not in card and "SYNTHETIC_FAKE_TOKEN" not in card
    parquet = directory / "data/noaa_aggregate_2024-01.parquet"
    assert parquet.read_bytes() == candidate.read_bytes()
    assert sha256(candidate) == before  # original candidate preserved, not regenerated
    assert pq.ParquetFile(parquet).schema_arrow.equals(PUBLIC_SCHEMA, check_metadata=True)
    assert pq.ParquetFile(parquet).metadata.num_rows == 1
    assert set(pq.ParquetFile(parquet).schema_arrow.names) == {
        "month", "region", "cargo_vessels", "observations"}
    assert package_local("2024-01", raw_root=raw, processed_root=processed, output_root=output) == result


def test_missing_or_tampered_noaa_provenance_is_rejected(tmp_path):
    raw, processed = prepared_month(tmp_path)
    output = tmp_path / "data/local_dataset"
    (raw / "noaa/2024-01-31-ny-nj.manifest.json").unlink()
    with pytest.raises(ValueError, match="manifest missing"):
        package_local("2024-01", raw_root=raw, processed_root=processed, output_root=output)
    assert not output.exists()


def test_candidate_extra_identifier_and_existing_package_tampering_fail(tmp_path):
    raw, processed = prepared_month(tmp_path)
    output = tmp_path / "data/local_dataset"
    result = package_local("2024-01", raw_root=raw, processed_root=processed, output_root=output)
    directory = Path(result["path"])
    (directory / "data" / "aisstream.jsonl").write_text("SYNTHETIC\n")
    with pytest.raises(ValueError, match="extra, missing"):
        package_local("2024-01", raw_root=raw, processed_root=processed, output_root=output)
    (directory / "data" / "aisstream.jsonl").unlink()
    candidate = processed / "noaa_candidate/2024-01-ny-nj.parquet"
    table = pq.read_table(candidate).append_column("mmsi", pa.array(["100000001"]))
    pq.write_table(table, candidate)
    manifest = candidate.with_suffix(".manifest.json")
    metadata = json.loads(manifest.read_text())
    metadata["sha256"] = sha256(candidate)
    manifest.write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match="unexpected columns"):
        package_local("2024-01", raw_root=raw, processed_root=processed, output_root=output)
    with pytest.raises(ValueError, match="2024 calendar month"):
        package_local("2025-01", raw_root=raw, processed_root=processed, output_root=output)

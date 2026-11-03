"""DB tests use an isolated *_test database and synthetic vessel observations."""

import json
import os
import re
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.load.postgres import _checked_manifest, _connection_url, _rows, load, load_candidate
from scripts.transform.batches import sha256, transform
from test_transform import ais_input, noaa_input, noaa_row


def fixtures(tmp_path):
    raw, processed = tmp_path / "raw", tmp_path / "processed"
    day = "2024-01-14"
    noaa = noaa_input(raw, day, [noaa_row(day), noaa_row(day, mmsi=100000002, hour=13)])
    noaa_meta = transform(noaa, root=processed)
    ais_meta = transform(ais_input(raw), root=processed)
    return raw, processed, [
        processed / "source/noaa/manifests/2024-01-14-ny-nj.json",
        processed / "source/aisstream/manifests/synthetic-session.json",
    ], noaa_meta, ais_meta


def test_manifests_enforce_source_provenance_and_no_candidate_as_detail(tmp_path):
    raw, processed, paths, noaa_meta, _ = fixtures(tmp_path)
    checked, files, url = _checked_manifest(paths[0], raw, processed)
    assert checked["source"] == "noaa" and len(files) == 1
    assert url.startswith("https://noaaocm.blob.core.windows.net/")
    vessels, observations = next(_rows(files[0], "noaa", url, batch_size=2))
    assert len(vessels) == 2 and len(observations) == 2
    data = json.loads(paths[0].read_text())
    data["files"] = ["source/aisstream/date=2024-01-14/synthetic-session.parquet"]
    paths[0].write_text(json.dumps(data))
    with pytest.raises(ValueError, match="source or UTC partition"):
        _checked_manifest(paths[0], raw, processed)
    with pytest.raises(ValueError, match="only locally generated NOAA candidate"):
        load_candidate(files[0], raw_root=raw, processed_root=processed)


def test_remote_database_is_rejected_before_connection():
    with pytest.raises(ValueError, match="remote PostgreSQL forbidden"):
        _connection_url("postgresql://user:dummy@192.0.2.1:5432/ais")
    with pytest.raises(ValueError, match="remote PostgreSQL forbidden"):
        _connection_url("postgresql://user:dummy@localhost/ais?hostaddr=192.0.2.1")
    assert _connection_url("postgresql://user:dummy@127.0.0.1:5432/ais").startswith("postgresql://")


def test_changed_raw_input_prevents_load(tmp_path):
    raw, processed, paths, _, _ = fixtures(tmp_path)
    with (raw / "noaa/2024-01-14-ny-nj.jsonl").open("a") as file:
        file.write("\n")
    with pytest.raises(ValueError, match="checksum changed"):
        load(paths[0], raw_root=raw, processed_root=processed, url="not-used")


def _analysis_queries():
    text = Path("sql/analyses.sql").read_text()
    headings = list(re.finditer(r"(?m)^-- (0[1-5])\. ", text))
    assert [m.group(1) for m in headings] == ["01", "02", "03", "04", "05"]
    for i, heading in enumerate(headings):
        section = text[heading.start():headings[i + 1].start() if i + 1 < len(headings) else len(text)]
        query = "\n".join(line for line in section.splitlines() if not line.startswith("--"))
        assert query.strip().endswith(";")
        yield query


def test_five_documented_queries_exist():
    assert len(list(_analysis_queries())) == 5


@pytest.mark.skipif(not os.environ.get("TEST_DATABASE_URL"), reason="isolated PostgreSQL *_test URL not configured")
def test_postgres_idempotency_constraints_and_five_queries(tmp_path):
    url = os.environ["TEST_DATABASE_URL"]
    with psycopg.connect(url, autocommit=True) as conn:
        if not conn.info.dbname.endswith("_test"):
            pytest.fail("TEST_DATABASE_URL must point to dedicated *_test DB")
        conn.execute(Path("sql/schema.sql").read_text())
        conn.execute("TRUNCATE observations, vessels, ingestion_batches, noaa_aggregates RESTART IDENTITY CASCADE")
    raw, processed, manifests, _, _ = fixtures(tmp_path)
    assert load(manifests[0], raw_root=raw, processed_root=processed, url=url, batch_size=1)["inserted_observations"] == 2
    assert load(manifests[1], raw_root=raw, processed_root=processed, url=url)["inserted_observations"] == 1
    assert load(manifests[0], raw_root=raw, processed_root=processed, url=url)["inserted_observations"] == 0
    assert load(manifests[1], raw_root=raw, processed_root=processed, url=url)["inserted_observations"] == 0
    with psycopg.connect(url, autocommit=True) as conn:
        assert conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM vessels").fetchone()[0] == 3  # same MMSI across sources
        assert conn.execute("SELECT COUNT(*) FROM ingestion_batches").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM noaa_aggregates").fetchone()[0] == 0
        for query in _analysis_queries():
            conn.execute(query).fetchall()
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute("UPDATE observations SET latitude = 999 WHERE source_code = 'noaa'")
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute("UPDATE observations SET vessel_type = 80 WHERE source_code = 'noaa'")
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute("UPDATE observations SET source_url = NULL WHERE source_code = 'noaa'")
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute("INSERT INTO noaa_aggregates(month_start,region,source_code,cargo_vessels,observations,candidate_sha256) VALUES ('2024-01-01','ny-nj','aisstream',20,200,%s)", ("a" * 64,))
        assert conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 3


@pytest.mark.skipif(not os.environ.get("TEST_DATABASE_URL"), reason="isolated PostgreSQL *_test URL not configured")
def test_transaction_rolls_back_an_invalid_second_batch(tmp_path):
    url = os.environ["TEST_DATABASE_URL"]
    with psycopg.connect(url, autocommit=True) as conn:
        if not conn.info.dbname.endswith("_test"):
            pytest.fail("TEST_DATABASE_URL must point to dedicated *_test DB")
        conn.execute(Path("sql/schema.sql").read_text())
        conn.execute("TRUNCATE observations, vessels, ingestion_batches, noaa_aggregates RESTART IDENTITY CASCADE")
    raw, processed, manifests, meta, _ = fixtures(tmp_path)
    parquet = processed / meta["files"][0]
    records = pq.ParquetFile(parquet).read().to_pylist()
    records[1]["source"] = "aisstream"  # forged second row, after first batch inserted
    pq.write_table(pa.Table.from_pylist(records, schema=pq.ParquetFile(parquet).schema_arrow), parquet)
    metadata = json.loads(manifests[0].read_text())
    metadata["files_sha256"][meta["files"][0]] = sha256(parquet)
    manifests[0].write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match="row source"):
        load(manifests[0], raw_root=raw, processed_root=processed, url=url, batch_size=1)
    with psycopg.connect(url) as conn:
        assert conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM vessels").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM ingestion_batches").fetchone()[0] == 0

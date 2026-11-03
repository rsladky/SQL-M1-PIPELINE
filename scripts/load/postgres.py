"""Chargement PostgreSQL LOCAL et transactionnel, depuis les manifests validés.

Ne lit jamais directement data/raw/*.jsonl ni un Parquet candidat comme observation.
Aucune donnée de navire n'est publiée ou exposée par ce programme.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

import psycopg
import pyarrow.parquet as pq
from psycopg.conninfo import conninfo_to_dict

from scripts.transform.aggregate import PUBLIC_SCHEMA, candidate
from scripts.transform.batches import SCHEMA, _verified_noaa_manifest, sha256

MAX_BATCH = 5000
SOURCE_URL = "https://noaaocm.blob.core.windows.net/ais/csv2/"
INSERT_VESSEL = """INSERT INTO vessels (source_code,mmsi,imo,vessel_name) VALUES (%s,%s,%s,%s)
    ON CONFLICT (source_code,mmsi) DO NOTHING"""
INSERT_OBSERVATION = """INSERT INTO observations
    (source_code,mmsi,observed_at,latitude,longitude,vessel_type,sog,cog,heading,quality_flag,source_url)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (source_code,mmsi,observed_at,latitude,longitude) DO NOTHING"""


def _connection_url(url: str | None) -> str:
    value = url or os.environ.get("DATABASE_URL")
    if not value:
        raise RuntimeError("DATABASE_URL is required; PostgreSQL must remain local")
    options = conninfo_to_dict(value)
    if (options.get("host") not in ("127.0.0.1", "localhost", "::1") or
            options.get("hostaddr", "127.0.0.1") not in ("127.0.0.1", "::1") or
            options.get("service")):
        raise ValueError("remote PostgreSQL forbidden: DATABASE_URL must target loopback explicitly")
    return value


def _checked_manifest(manifest_path: Path, raw_root: Path, processed_root: Path) -> tuple[dict, list[Path], str | None]:
    """Validate file membership, hashes and provenance BEFORE starting SQL transaction."""
    root = processed_root.resolve()
    manifest = manifest_path.resolve()
    if not manifest.is_relative_to(root):
        raise ValueError("processed manifest outside local processed root")
    data = json.loads(manifest.read_text())
    source = data.get("source")
    stem = Path(str(data.get("input", ""))).stem
    if source not in ("noaa", "aisstream") or manifest != root / "source" / source / "manifests" / f"{stem}.json":
        raise ValueError("invalid source or transform manifest path")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+\.jsonl", str(data.get("input", ""))):
        raise ValueError("invalid raw filename")
    raw = raw_root / source / data["input"]
    if not raw.is_file() or data["input_sha256"] != sha256(raw):
        raise ValueError("raw input missing or checksum changed")
    source_url = None
    if source == "noaa":
        match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})-([a-z0-9-]+)", stem)
        if not match or data.get("date") != match.group(1) or data.get("zone") != match.group(2):
            raise ValueError("NOAA day/zone mismatch")
        ingested = _verified_noaa_manifest(raw, data["input_sha256"], *match.groups())
        if data.get("noaa_ingest_sha256") != ingested["sha256"]:
            raise ValueError("NOAA transform/ingestion provenance mismatch")
        source_url = ingested["url"]
    else:
        if data.get("noaa_ingest_sha256") is not None:
            raise ValueError("AISStream cannot claim NOAA provenance")
        session = json.loads(raw.with_suffix(".manifest.json").read_text())
        if session.get("source") != "aisstream" or session.get("session_id") != stem:
            raise ValueError("invalid AISStream finished session")
    files: list[Path] = []
    for relative in data["files"]:
        if not isinstance(relative, str):
            raise ValueError("invalid partition filename")
        match = re.fullmatch(rf"source/{source}/date=(\d{{4}}-\d{{2}}-\d{{2}})/{re.escape(stem)}\.parquet", relative)
        if not match or (source == "noaa" and data["date"] != match.group(1)):
            raise ValueError("source or UTC partition mismatch")
        file = root / relative
        if not file.is_file() or sha256(file) != data["files_sha256"][relative]:
            raise ValueError("partition missing or modified")
        files.append(file)
    if len(files) != len(set(files)):
        raise ValueError("duplicate partition listed")
    return data, files, source_url


def _rows(file: Path, source: str, source_url: str | None, *, batch_size: int):
    parquet = pq.ParquetFile(file)
    if not parquet.schema_arrow.equals(SCHEMA):
        raise ValueError("unexpected detailed Parquet schema")
    day = file.parent.name.removeprefix("date=")
    for batch in parquet.iter_batches(batch_size=batch_size):
        observations, vessels = [], {}
        for row in batch.to_pylist():
            when = row["observed_at"]
            if (row["source"] != source or row["source_url"] != source_url or
                    when is None or when.tzinfo is None or when.date().isoformat() != day or
                    not re.fullmatch(r"[0-9]{9}", row["mmsi"] or "") or
                    row["vessel_type"] not in range(70, 80)):
                raise ValueError("Parquet row source, date, identity or cargo classification mismatch")
            if source == "noaa" and (not source_url or not source_url.startswith(SOURCE_URL)):
                raise ValueError("NOAA provenance URL missing")
            if row["quality_flag"] not in ("ok", "tracking_gap", "implausible_jump", "conflicting_position"):
                raise ValueError("unknown quality flag")
            vessels[row["mmsi"]] = (source, row["mmsi"], row["imo"], row["vessel_name"])
            observations.append((source, row["mmsi"], when, row["latitude"], row["longitude"],
                                 row["vessel_type"], row["sog"], row["cog"], row["heading"],
                                 row["quality_flag"], row["source_url"]))
        yield list(vessels.values()), observations


def load(
    manifest_path: Path, *, raw_root: Path = Path("data/raw"),
    processed_root: Path = Path("data/processed"), url: str | None = None,
    batch_size: int = MAX_BATCH,
) -> dict:
    """One transaction per transform manifest; rerunning unchanged data is idempotent."""
    if not 1 <= batch_size <= MAX_BATCH:
        raise ValueError("batch size must be 1..5000")
    data, files, origin = _checked_manifest(manifest_path, raw_root, processed_root)
    source = data["source"]
    inserted = 0
    with psycopg.connect(_connection_url(url), autocommit=True) as conn, conn.transaction():
        with conn.cursor() as cur:
            for file in files:
                relative = file.relative_to(processed_root.resolve()).as_posix()
                existing = cur.execute("""SELECT input_sha256, parquet_sha256 FROM ingestion_batches
                    WHERE source_code=%s AND input_name=%s AND parquet_path=%s""",
                    (source, data["input"], relative)).fetchone()
                digest = data["files_sha256"][relative]
                if existing and existing != (data["input_sha256"], digest):
                    raise ValueError("input changed after earlier load: rebuild database explicitly")
                if existing:
                    continue
                for vessels, observations in _rows(file, source, origin, batch_size=batch_size):
                    cur.executemany(INSERT_VESSEL, vessels)
                    cur.executemany(INSERT_OBSERVATION, observations)
                    inserted += cur.rowcount
                cur.execute("""INSERT INTO ingestion_batches
                    (source_code,input_name,input_sha256,parquet_path,parquet_sha256,source_url)
                    VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (source, data["input"], data["input_sha256"], relative, digest, origin))
    return {"source": source, "files": len(files), "inserted_observations": inserted}


def load_candidate(
    path: Path, *, raw_root: Path = Path("data/raw"),
    processed_root: Path = Path("data/processed"), url: str | None = None,
) -> dict:
    """Local-only: regenerate candidate to recheck every month/day and privacy threshold."""
    expected_root = (processed_root / "noaa_candidate").resolve()
    path = path.resolve()
    if path.parent != expected_root or not re.fullmatch(r"\d{4}-\d{2}-ny-nj\.parquet", path.name):
        raise ValueError("only locally generated NOAA candidate Parquet is accepted")
    month = path.name[:7]
    result = candidate(month, raw_root=raw_root, processed_root=processed_root)
    if result.get("status") != "candidate_not_approved_for_publication":
        raise ValueError(f"NOAA candidate suppressed: {result.get('reason')}")
    manifest = json.loads(path.with_suffix(".manifest.json").read_text())
    if manifest != result or manifest["sha256"] != sha256(path):
        raise ValueError("NOAA candidate provenance/hash mismatch")
    parquet = pq.ParquetFile(path)
    if not parquet.schema_arrow.equals(PUBLIC_SCHEMA) or parquet.metadata.num_rows != 1:
        raise ValueError("NOAA candidate schema/row count mismatch")
    record = parquet.read().to_pylist()[0]
    if record["month"] != month or record["region"] != "ny-nj":
        raise ValueError("NOAA candidate period/region mismatch")
    with psycopg.connect(_connection_url(url), autocommit=True) as conn, conn.transaction():
        conn.execute("""INSERT INTO noaa_aggregates
            (month_start,region,source_code,cargo_vessels,observations,candidate_sha256)
            VALUES (%s,%s,'noaa',%s,%s,%s)
            ON CONFLICT (month_start,region) DO UPDATE SET
                cargo_vessels=EXCLUDED.cargo_vessels,
                observations=EXCLUDED.observations,
                candidate_sha256=EXCLUDED.candidate_sha256""",
            (month + "-01", "ny-nj", record["cargo_vessels"], record["observations"], manifest["sha256"]))
    return {"status": "loaded_private_noaa_candidate", "month": month}


def main() -> None:
    parser = argparse.ArgumentParser(description="Load ONLY local source Parquet or NOAA candidate")
    parser.add_argument("manifests", nargs="*", type=Path, help="Detailed transform manifest(s)")
    parser.add_argument("--aggregate", type=Path, help="Optional local NOAA candidate; never publishes")
    parser.add_argument("--batch-size", type=int, default=MAX_BATCH)
    args = parser.parse_args()
    if not args.manifests and not args.aggregate:
        parser.error("provide detailed manifest(s) and/or --aggregate")
    for manifest in args.manifests:
        print(json.dumps(load(manifest, batch_size=args.batch_size)))
    if args.aggregate:
        print(json.dumps(load_candidate(args.aggregate)))


if __name__ == "__main__":
    main()

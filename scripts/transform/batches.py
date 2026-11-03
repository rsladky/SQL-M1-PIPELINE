"""Transformations AIS locales : JSONL -> Parquet par source et jour UTC.

SQLite temporaire borne la RAM et assure une déduplication exacte par fichier.
Ni les données détaillées ni le produit candidat ne doivent être commités.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

RAW_NOAA = re.compile(r"^(\d{4}-\d{2}-\d{2})-([a-z0-9-]+)\.jsonl$")
SOURCE_NOAA = "https://noaaocm.blob.core.windows.net/ais/csv2/"
BATCH_SIZE = 5000
MAX_LINE = 2_000_000
GAP_SECONDS = 6 * 3600
MAX_KNOTS = 60.0
SCHEMA = pa.schema([
    ("source", pa.string()), ("source_url", pa.string()),
    ("mmsi", pa.string()), ("imo", pa.string()), ("vessel_name", pa.string()),
    ("vessel_type", pa.int16()), ("observed_at", pa.timestamp("us", tz="UTC")),
    ("latitude", pa.float64()), ("longitude", pa.float64()),
    ("sog", pa.float32()), ("cog", pa.float32()), ("heading", pa.int16()),
    ("quality_flag", pa.string()),
])


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for part in iter(lambda: file.read(256 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def utc(text: str) -> datetime:
    if not isinstance(text, str) or not text:
        raise ValueError("missing timestamp")
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("timestamp has no timezone")
    return dt.astimezone(timezone.utc)


def number(value, *, low: float, high: float) -> float | None:
    if value is None or str(value).strip() in ("", "None"):
        return None
    try:
        n = float(value)
    except (ValueError, TypeError):
        return None
    return n if math.isfinite(n) and low <= n <= high else None


def _row(raw: dict, source: str, expected_date: str | None) -> tuple:
    if raw.get("source") != source or not isinstance(raw.get("record"), dict):
        raise ValueError("source/provenance mismatch")
    record = raw["record"]
    if source == "noaa":
        url = raw.get("source_url")
        if not isinstance(url, str) or not url.startswith(SOURCE_NOAA):
            raise ValueError("invalid NOAA provenance URL")
        if not expected_date or not url.endswith(f"/ais-{expected_date}.csv.zst"):
            raise ValueError("NOAA URL and input day mismatch")
        observed = utc(raw["observed_at"])
        if observed.date().isoformat() != expected_date:
            raise ValueError("NOAA observation outside archive day")
        mmsi = str(record.get("mmsi") or record.get("MMSI") or "")
        lat = record.get("latitude", record.get("LAT"))
        lon = record.get("longitude", record.get("LON"))
        imo, name = record.get("imo", record.get("IMO")), record.get("vessel_name", record.get("VesselName"))
        sog, cog, heading = record.get("sog", record.get("SOG")), record.get("cog", record.get("COG")), record.get("heading", record.get("Heading"))
        vessel_type = record.get("vessel_type", record.get("VesselType"))
    else:
        if raw.get("source_url"):
            raise ValueError("AISStream record falsely claims an archive URL")
        observed = utc(raw["received_at"])
        metadata = record.get("MetaData") or {}
        if not isinstance(metadata, dict):
            raise ValueError("invalid AISStream metadata")
        mmsi = str(metadata.get("MMSI") or "")
        lat, lon = metadata.get("Latitude"), metadata.get("Longitude")
        imo, name = None, metadata.get("ShipName")
        kind = record.get("MessageType")
        if kind not in ("PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"):
            raise ValueError("AISStream record is not a position")
        position = (record.get("Message") or {}).get(kind) or {}
        sog, cog, heading = position.get("Sog"), position.get("Cog"), position.get("TrueHeading")
        vessel_type = raw.get("vessel_type")
        url = None
    if not re.fullmatch(r"\d{9}", mmsi):
        raise ValueError("invalid MMSI")
    if not isinstance(vessel_type, (str, int)) or not str(vessel_type).isdigit():
        raise ValueError("invalid vessel type")
    kind_int = int(vessel_type)
    if kind_int not in range(70, 80):
        raise LookupError("not cargo")
    latitude, longitude = number(lat, low=-90, high=90), number(lon, low=-180, high=180)
    if latitude is None or longitude is None or (latitude == 0 and longitude == 0):
        raise ValueError("invalid AIS position")
    speed = number(sog, low=0, high=60)  # 102.3 sentinel, negatives and implausible speeds -> null
    course = number(cog, low=0, high=359.9)  # 360 sentinel
    hdg = number(heading, low=0, high=359)  # 511 sentinel
    return (
        source, url, mmsi, str(imo or "") or None, str(name or "") or None,
        kind_int, observed.isoformat(), latitude, longitude,
        speed, course, int(hdg) if hdg is not None else None,
    )


def _distance_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    a, b = map(math.radians, (lat1, lat2))
    c, d = map(math.radians, (lon1, lon2))
    h = math.sin((b-a)/2)**2 + math.cos(a)*math.cos(b)*math.sin((d-c)/2)**2
    return 3440.065 * 2 * math.asin(min(1.0, math.sqrt(h)))


def _source_for(path: Path) -> tuple[str, str | None, str]:
    source = path.parent.name
    if source not in ("noaa", "aisstream"):
        raise ValueError("input must be within raw/noaa or raw/aisstream")
    if source == "noaa":
        match = RAW_NOAA.fullmatch(path.name)
        if not match:
            raise ValueError("NOAA file must be named YYYY-MM-DD-zone.jsonl")
        return source, match.group(1), match.group(2)
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}\.jsonl", path.name):
        raise ValueError("unsafe AISStream session filename")
    return source, None, "local-only"


def _verified_noaa_manifest(path: Path, raw_sha: str, day: str, zone: str) -> dict:
    manifest = path.with_suffix(".manifest.json")
    if not manifest.exists():
        raise ValueError("NOAA ingestion manifest missing")
    meta = json.loads(manifest.read_text())
    if (meta.get("source") != "noaa" or meta.get("date") != day or meta.get("zone") != zone
            or meta.get("sha256") != raw_sha or meta.get("output_bytes") != path.stat().st_size
            or meta.get("url") != f"{SOURCE_NOAA}csv{day[:4]}/ais-{day}.csv.zst"):
        raise ValueError("NOAA manifest checksum or provenance mismatch")
    return meta


def transform(path: Path, *, root: Path = Path("data/processed"), batch_size: int = BATCH_SIZE) -> dict:
    """Déterministe par fichier d'entrée ; aucun corpus entier chargé en mémoire."""
    if not 1 <= batch_size <= 50_000:
        raise ValueError("batch size must be 1..50000")
    source, day, zone = _source_for(path)
    input_sha = sha256(path)
    input_meta = _verified_noaa_manifest(path, input_sha, day, zone) if source == "noaa" else None
    if source == "aisstream" and not path.with_suffix(".manifest.json").exists():
        raise ValueError("AISStream finished-session manifest missing")
    if source == "aisstream":
        session = json.loads(path.with_suffix(".manifest.json").read_text())
        if session.get("source") != "aisstream" or session.get("session_id") != path.stem:
            raise ValueError("AISStream session provenance mismatch")
    # Destination is keyed by input; staging is private and never scanned as published data.
    # Avoid hive partition name `source=`: Arrow would try to merge that
    # inferred dictionary column with the detailed string `source` field.
    base = root / "source" / source
    root.mkdir(parents=True, exist_ok=True)
    processed_manifest = base / "manifests" / f"{path.stem}.json"
    if processed_manifest.exists():
        try:
            prior = json.loads(processed_manifest.read_text())
            if (prior["input_sha256"] == input_sha and
                    all((root / p).exists() and sha256(root / p) == prior["files_sha256"][p]
                        for p in prior["files"])):
                return prior
        except (OSError, KeyError, ValueError):
            pass
    with tempfile.TemporaryDirectory(prefix=".transform-", dir=root) as temp:
        temp_dir = Path(temp)
        db = sqlite3.connect(temp_dir / "dedup.sqlite")
        db.execute("PRAGMA temp_store=FILE")
        db.execute("PRAGMA cache_size=-4096")  # approx 4 MiB page cache
        db.execute("""CREATE TABLE obs (
            source TEXT, source_url TEXT, mmsi TEXT, imo TEXT, name TEXT,
            vessel_type INTEGER, observed_at TEXT, lat REAL, lon REAL,
            sog REAL, cog REAL, heading INTEGER, quality TEXT DEFAULT 'ok',
            PRIMARY KEY (mmsi, observed_at, lat, lon))""")
        counts = {"input": 0, "cargo": 0, "noncargo": 0, "invalid": 0, "duplicates": 0, "quality_flags": 0}
        try:
            with path.open("rb") as file:
                while line := file.readline(MAX_LINE + 1):
                    counts["input"] += 1
                    if len(line) > MAX_LINE:
                        while line and not line.endswith(b"\n"):
                            line = file.readline(MAX_LINE + 1)
                        counts["invalid"] += 1
                        continue
                    try:
                        row = _row(json.loads(line), source, day)
                    except LookupError:
                        counts["noncargo"] += 1
                        continue
                    except (ValueError, TypeError, KeyError, OverflowError):
                        counts["invalid"] += 1
                        continue
                    cur = db.execute("""INSERT OR IGNORE INTO obs (
                        source,source_url,mmsi,imo,name,vessel_type,observed_at,lat,lon,sog,cog,heading
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", row)
                    if cur.rowcount:
                        counts["cargo"] += 1
                    else:
                        counts["duplicates"] += 1
                    if counts["input"] % batch_size == 0:
                        db.commit()
            db.commit()
            # Source-scoped vessel identities; a MMSI can be reassigned. Gaps and
            # implausible jumps are flags, not inferred continuous voyages.
            previous: tuple | None = None
            current_mmsi = None
            for rid, mmsi, when, lat, lon in db.execute(
                "SELECT rowid,mmsi,observed_at,lat,lon FROM obs ORDER BY mmsi,observed_at,lat,lon"):
                if mmsi != current_mmsi:
                    previous = None
                    current_mmsi = mmsi
                flag = "ok"
                if previous is not None:
                    last_when, last_lat, last_lon = previous
                    seconds = (datetime.fromisoformat(when) - datetime.fromisoformat(last_when)).total_seconds()
                    if seconds == 0 and (lat, lon) != (last_lat, last_lon):
                        flag = "conflicting_position"
                    elif seconds > 0 and _distance_nm(last_lat, last_lon, lat, lon) > MAX_KNOTS * seconds / 3600:
                        flag = "implausible_jump"
                    elif seconds > GAP_SECONDS:
                        flag = "tracking_gap"
                if flag != "ok":
                    db.execute("UPDATE obs SET quality=? WHERE rowid=?", (flag, rid))
                    counts["quality_flags"] += 1
                if flag not in ("implausible_jump", "conflicting_position"):
                    previous = (when, lat, lon)
            db.commit()
            # One writer per UTC day, but bounded by one input day (NOAA) or a
            # 15-minute session crossing midnight (AISStream).
            writers: dict[str, pq.ParquetWriter] = {}
            buffers: dict[str, list[dict]] = {}
            files: dict[str, Path] = {}
            try:
                cursor = db.execute("""SELECT source,source_url,mmsi,imo,name,vessel_type,
                    observed_at,lat,lon,sog,cog,heading,quality FROM obs
                    ORDER BY observed_at,mmsi,lat,lon""")
                for record in cursor:
                    observed = datetime.fromisoformat(record[6])
                    partition = observed.date().isoformat()
                    if len(writers) >= 3 and partition not in writers:
                        raise ValueError("too many UTC days in one input")
                    if partition not in writers:
                        staging = temp_dir / f"date={partition}.parquet"
                        files[partition] = staging
                        writers[partition] = pq.ParquetWriter(staging, SCHEMA, compression="zstd")
                        buffers[partition] = []
                    item = dict(zip(SCHEMA.names, record))
                    item["observed_at"] = observed
                    buffers[partition].append(item)
                    if len(buffers[partition]) >= batch_size:
                        writers[partition].write_table(pa.Table.from_pylist(buffers[partition], schema=SCHEMA))
                        buffers[partition].clear()
                for partition, writer in writers.items():
                    if buffers[partition]:
                        writer.write_table(pa.Table.from_pylist(buffers[partition], schema=SCHEMA))
            finally:
                for writer in writers.values():
                    writer.close()
        finally:
            db.close()
        published: list[str] = []
        hashes: dict[str, str] = {}
        for partition, staged in sorted(files.items()):
            relative = f"source/{source}/date={partition}/{path.stem}.parquet"
            destination = root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            checksum = sha256(staged)
            staged.replace(destination)
            published.append(relative)
            hashes[relative] = checksum
        metadata = {"source": source, "zone": zone, "input": path.name,
                    "input_sha256": input_sha, "date": day, "files": published,
                    "files_sha256": hashes, "counts": counts,
                    "noaa_ingest_sha256": input_meta["sha256"] if input_meta else None}
        processed_manifest.parent.mkdir(parents=True, exist_ok=True)
        tmp_manifest = processed_manifest.with_suffix(".tmp")
        tmp_manifest.write_text(json.dumps(metadata, indent=2) + "\n")
        tmp_manifest.replace(processed_manifest)
        return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description="Local-only AIS JSONL -> Parquet")
    parser.add_argument("inputs", nargs="+", type=Path, help="Completed raw/noaa or raw/aisstream JSONL")
    parser.add_argument("--root", type=Path, default=Path("data/processed"))
    args = parser.parse_args()
    for path in args.inputs:
        print(json.dumps(transform(path, root=args.root)["counts"]))


if __name__ == "__main__":
    main()

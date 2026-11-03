"""Produit candidat NOAA seulement, jamais publié automatiquement.

Exige tous les fichiers journaliers d'un mois calendaire dans une seule large
zone, leur provenance et des seuils de confidentialité. Le pilote d'un jour
reste obligatoirement supprimé. Une revue humaine/juridique reste nécessaire.
"""

from __future__ import annotations

import argparse
import calendar
import json
import re
import sqlite3
import tempfile
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.transform.batches import SCHEMA, _verified_noaa_manifest, sha256

MIN_VESSELS = 20
MIN_OBSERVATIONS = 200
MIN_OBSERVED_DAYS = 7
MAX_VESSEL_FRACTION = 0.10
PUBLIC_SCHEMA = pa.schema([
    ("month", pa.string()), ("region", pa.string()),
    ("cargo_vessels", pa.int64()), ("observations", pa.int64()),
])


def candidate(
    month: str, *, zone: str = "ny-nj", raw_root: Path = Path("data/raw"),
    processed_root: Path = Path("data/processed"),
) -> dict:
    """Returns a suppression reason or writes one NOAA-only *candidate* Parquet.

    No code path reads AISStream. A month is complete only if all calendar days
    have verified NOAA ingestion + transformation manifests for this region.
    """
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month) or zone != "ny-nj":
        raise ValueError("month YYYY-MM and pre-approved broad NY/NJ zone required")
    year, month_number = map(int, month.split("-"))
    days = calendar.monthrange(year, month_number)[1]
    # Never leave an earlier candidate accessible if evidence is later removed.
    output = processed_root / "noaa_candidate" / f"{month}-{zone}.parquet"
    output.unlink(missing_ok=True)
    output.with_suffix(".manifest.json").unlink(missing_ok=True)
    partitions: list[Path] = []
    missing: list[str] = []
    for n in range(1, days + 1):
        day = date(year, month_number, n).isoformat()
        stem = f"{day}-{zone}"
        raw = raw_root / "noaa" / f"{stem}.jsonl"
        processed_manifest = processed_root / "source" / "noaa" / "manifests" / f"{stem}.json"
        if not raw.exists() or not raw.with_suffix(".manifest.json").exists() or not processed_manifest.exists():
            missing.append(day)
            continue
        digest = sha256(raw)
        ingest_meta = _verified_noaa_manifest(raw, digest, day, zone)
        meta = json.loads(processed_manifest.read_text())
        if (meta.get("source") != "noaa" or meta.get("date") != day or
                meta.get("zone") != zone or meta.get("input_sha256") != digest or
                meta.get("noaa_ingest_sha256") != ingest_meta["sha256"]):
            raise ValueError("NOAA transform provenance mismatch")
        for relative in meta.get("files", []):
            expected = f"source/noaa/date={day}/{stem}.parquet"
            if relative != expected:
                raise ValueError("unexpected partition path")
            part = processed_root / relative
            if not part.is_file() or sha256(part) != meta["files_sha256"][relative]:
                raise ValueError("partition missing or checksum mismatch")
            partitions.append(part)
    if missing:
        return {"status": "suppressed", "reason": "incomplete_month", "missing_days": len(missing)}
    with tempfile.TemporaryDirectory(prefix=".aggregate-", dir=processed_root) as scratch:
        db = sqlite3.connect(Path(scratch) / "counts.sqlite")
        db.execute("PRAGMA cache_size=-2048")
        db.execute("""CREATE TABLE vessel_day (
            mmsi TEXT NOT NULL, day TEXT NOT NULL, count INTEGER NOT NULL,
            PRIMARY KEY(mmsi,day))""")
        try:
            for part in partitions:
                parquet = pq.ParquetFile(part)
                if not parquet.schema_arrow.equals(SCHEMA):
                    raise ValueError("detailed Parquet schema mismatch")
                for batch in parquet.iter_batches(batch_size=5000,
                                                   columns=["source", "mmsi", "observed_at", "quality_flag"]):
                    for record in batch.to_pylist():
                        when = record["observed_at"]
                        if (record["source"] != "noaa" or
                                when.strftime("%Y-%m") != month or
                                when.date().isoformat() != part.parent.name.removeprefix("date=")):
                            raise ValueError("non-NOAA or wrongly partitioned row")
                        if record["quality_flag"] in ("implausible_jump", "conflicting_position"):
                            continue
                        if record["quality_flag"] not in ("ok", "tracking_gap"):
                            raise ValueError("unknown quality flag")
                        db.execute("""INSERT INTO vessel_day(mmsi,day,count) VALUES (?,?,1)
                            ON CONFLICT(mmsi,day) DO UPDATE SET count=count+1""",
                            (record["mmsi"], when.date().isoformat()))
                db.commit()
            vessels, observations, observed_days = db.execute(
                "SELECT COUNT(DISTINCT mmsi), COALESCE(SUM(count),0), COUNT(DISTINCT day) FROM vessel_day"
            ).fetchone()
            largest = db.execute("SELECT COALESCE(MAX(total),0) FROM (SELECT SUM(count) AS total FROM vessel_day GROUP BY mmsi)").fetchone()[0]
        finally:
            db.close()
    if (vessels < MIN_VESSELS or observations < MIN_OBSERVATIONS or
            observed_days < MIN_OBSERVED_DAYS or largest > MAX_VESSEL_FRACTION * observations):
        return {"status": "suppressed", "reason": "privacy_thresholds",
                "policy": "20 vessels, 200 observations, 7 observed days, max 10% per vessel"}
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(".parquet.part")
    try:
        table = pa.Table.from_pylist([{
            "month": month, "region": zone,
            "cargo_vessels": vessels, "observations": observations,
        }], schema=PUBLIC_SCHEMA)
        pq.write_table(table, temp, compression="zstd")
        temp.replace(output)
        metadata = {"source": "noaa", "status": "candidate_not_approved_for_publication",
                    "month": month, "region": zone, "sha256": sha256(output),
                    "columns": PUBLIC_SCHEMA.names,
                    "note": "Derived historical NOAA aggregate. Human legal/privacy review required."}
        manifest = output.with_suffix(".manifest.json")
        manifest.write_text(json.dumps(metadata, indent=2) + "\n")
        return metadata
    finally:
        temp.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Local-only NOAA monthly candidate (not publication)")
    parser.add_argument("month", help="YYYY-MM; all days of month must be processed")
    args = parser.parse_args()
    print(json.dumps(candidate(args.month), indent=2))


if __name__ == "__main__":
    main()

"""Préparer UNIQUEMENT un dossier Dataset Card + agrégat NOAA sous data/.

Aucun client Hub, jeton ou appel réseau. Le candidat original reste intact.
Ce dossier brouillon n'est ni une permission de diffusion ni une publication.
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import os
import re
import tempfile
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq

from scripts.ingest.noaa import PILOT_BOUNDS
from scripts.transform.aggregate import (
    MAX_VESSEL_FRACTION, MIN_OBSERVATIONS, MIN_OBSERVED_DAYS,
    MIN_VESSELS, PUBLIC_SCHEMA,
)
from scripts.transform.batches import SCHEMA, SOURCE_NOAA, _verified_noaa_manifest, sha256

TEMPLATE = Path(__file__).resolve().parents[2] / "docs" / "LOCAL_DATASET_CARD_TEMPLATE.md"
MONTH = re.compile(r"2024-(0[1-9]|1[0-2])\Z")
SOURCE = "noaa"
REGION = "ny-nj"
VALID_FLAGS = {"ok", "tracking_gap"}
EXCLUDED_FLAGS = {"implausible_jump", "conflicting_position"}
MAX_CANDIDATE_BYTES = 1_000_000


def _read_candidate(month: str, processed_root: Path) -> tuple[bytes, dict, dict]:
    file = processed_root / "noaa_candidate" / f"{month}-{REGION}.parquet"
    manifest = file.with_suffix(".manifest.json")
    if not file.is_file() or not manifest.is_file() or not 0 < file.stat().st_size <= MAX_CANDIDATE_BYTES:
        raise ValueError("NOAA monthly candidate and manifest missing or oversized")
    payload = file.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    meta = json.loads(manifest.read_text(encoding="utf-8"))
    if (set(meta) != {"source", "status", "month", "region", "sha256", "columns", "note"} or
            meta["source"] != SOURCE or meta["status"] != "candidate_not_approved_for_publication" or
            meta["month"] != month or meta["region"] != REGION or
            meta["sha256"] != digest or meta["columns"] != PUBLIC_SCHEMA.names):
        raise ValueError("candidate provenance, status, columns or SHA-256 mismatch")
    parquet = pq.ParquetFile(file)
    if (not parquet.schema_arrow.equals(PUBLIC_SCHEMA, check_metadata=True) or
            parquet.metadata.num_rows != 1 or
            set(parquet.metadata.metadata or {}) - {b"ARROW:schema"} or
            b"aisstream" in payload.lower()):
        raise ValueError("candidate contains unexpected columns, metadata, rows or source")
    row = parquet.read().to_pylist()[0]
    if (set(row) != set(PUBLIC_SCHEMA.names) or row["month"] != month or
            row["region"] != REGION or row["cargo_vessels"] < MIN_VESSELS or
            row["observations"] < MIN_OBSERVATIONS):
        raise ValueError("candidate values do not match NOAA monthly policy")
    return payload, row, meta


def _verify_month(month: str, raw_root: Path, processed_root: Path, row: dict) -> dict:
    """Read-only independent validation of all 31 raw/transformed daily units."""
    year, number = map(int, month.split("-"))
    per_vessel: Counter[str] = Counter()
    observed_days: set[str] = set()
    all_points = 0
    excluded = 0
    source_days = calendar.monthrange(year, number)[1]
    for day_number in range(1, source_days + 1):
        day = f"{month}-{day_number:02d}"
        stem = f"{day}-{REGION}"
        source_file = raw_root / SOURCE / f"{stem}.jsonl"
        if not source_file.is_file():
            raise ValueError(f"NOAA daily input missing: {day}")
        source_sha = sha256(source_file)
        source_meta = _verified_noaa_manifest(source_file, source_sha, day, REGION)
        if source_meta.get("bounds") != list(PILOT_BOUNDS):
            raise ValueError(f"NOAA pilot rectangle mismatch: {day}")
        source_url = f"{SOURCE_NOAA}csv{year}/ais-{day}.csv.zst"
        if source_meta["url"] != source_url:
            raise ValueError(f"NOAA archive URL mismatch: {day}")
        transform_meta_file = processed_root / "source" / SOURCE / "manifests" / f"{stem}.json"
        if not transform_meta_file.is_file():
            raise ValueError(f"NOAA transformed manifest missing: {day}")
        transform_meta = json.loads(transform_meta_file.read_text(encoding="utf-8"))
        if (transform_meta.get("source") != SOURCE or transform_meta.get("zone") != REGION or
                transform_meta.get("date") != day or transform_meta.get("input") != source_file.name or
                transform_meta.get("input_sha256") != source_sha or
                transform_meta.get("noaa_ingest_sha256") != source_sha):
            raise ValueError(f"NOAA transformed provenance mismatch: {day}")
        files = transform_meta.get("files")
        if not isinstance(files, list) or len(files) > 1:
            raise ValueError(f"NOAA expected <=1 daily detailed partition: {day}")
        for relative in files:
            expected = f"source/{SOURCE}/date={day}/{stem}.parquet"
            if relative != expected:
                raise ValueError(f"NOAA wrong partition path: {day}")
            part = processed_root / expected
            if not part.is_file() or sha256(part) != transform_meta["files_sha256"].get(relative):
                raise ValueError(f"NOAA detailed partition checksum mismatch: {day}")
            parquet = pq.ParquetFile(part)
            if not parquet.schema_arrow.equals(SCHEMA):
                raise ValueError(f"NOAA detailed partition schema mismatch: {day}")
            for batch in parquet.iter_batches(batch_size=5000, columns=[
                "source", "source_url", "mmsi", "observed_at", "vessel_type",
                "latitude", "longitude", "quality_flag",
            ]):
                for point in batch.to_pylist():
                    if (point["source"] != SOURCE or point["source_url"] != source_url or
                            point["observed_at"].date().isoformat() != day or
                            not re.fullmatch(r"\d{9}", point["mmsi"] or "") or
                            point["vessel_type"] not in range(70, 80) or
                            not (PILOT_BOUNDS[0] <= point["latitude"] <= PILOT_BOUNDS[2]) or
                            not (PILOT_BOUNDS[1] <= point["longitude"] <= PILOT_BOUNDS[3])):
                        raise ValueError(f"NOAA source, identity, coordinates or UTC date mismatch: {day}")
                    flag = point["quality_flag"]
                    if flag in VALID_FLAGS:
                        per_vessel[point["mmsi"]] += 1
                        observed_days.add(day)
                    elif flag in EXCLUDED_FLAGS:
                        excluded += 1
                    else:
                        raise ValueError(f"unknown NOAA quality flag: {day}")
                    all_points += 1
    valid = sum(per_vessel.values())
    largest = max(per_vessel.values(), default=0)
    if (row["cargo_vessels"] != len(per_vessel) or row["observations"] != valid or
            valid + excluded != all_points or len(observed_days) < MIN_OBSERVED_DAYS or
            largest > MAX_VESSEL_FRACTION * valid):
        raise ValueError("candidate count, observed days or contribution threshold failed")
    return {"calendar_days_verified": source_days, "observed_days": len(observed_days),
            "excluded_quality_points": excluded,
            "maximum_contribution_percent": round(100 * largest / valid, 3)}


def _render_card(month: str, days: int) -> bytes:
    text = TEMPLATE.read_text(encoding="utf-8")
    for marker, value in {
        "{{MONTH}}": month, "{{YEAR}}": month[:4], "{{DAYS}}": str(days),
        "{{DATA_FILE}}": f"data/noaa_aggregate_{month}.parquet",
    }.items():
        if marker not in text:
            raise ValueError(f"Dataset Card local template marker missing: {marker}")
        text = text.replace(marker, value)
    if re.search(r"\{\{[^}]+\}\}", text):
        raise ValueError("Dataset Card local contains unresolved placeholders")
    if "DRAFT LOCAL ONLY — NOT PUBLISHED" not in text or re.search(r"(?m)^license\s*:", text, re.I):
        raise ValueError("Dataset Card local status/license safety check failed")
    return text.encode("utf-8")


def _existing_package(target: Path, data_name: str, source_hash: str,
                      card: bytes, row: dict, audit: dict) -> dict:
    allowed = {"README.md", "manifest.json", "data"}
    if ({p.name for p in target.iterdir()} != allowed or
            not (target / "data").is_dir() or
            {p.name for p in (target / "data").iterdir()} != {data_name} or
            any(p.is_symlink() for p in (
                target / "README.md", target / "manifest.json", target / "data",
                target / "data" / data_name,
            ))):
        raise ValueError("existing local package contains extra, missing or linked files; inspect manually")
    manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    if (set(manifest) != {
            "status", "source", "month", "region", "source_candidate_sha256",
            "data_file", "data_sha256", "card_sha256", "columns", "rows",
            "cargo_vessels", "observations", *audit, "rights", "network_calls",
        } or
            manifest.get("status") != "draft_local_not_published" or
            manifest.get("source") != SOURCE or manifest.get("month") != row["month"] or
            manifest.get("region") != REGION or manifest.get("rows") != 1 or
            manifest.get("columns") != PUBLIC_SCHEMA.names or
            manifest.get("cargo_vessels") != row["cargo_vessels"] or
            manifest.get("observations") != row["observations"] or
            any(manifest.get(k) != value for k, value in audit.items()) or
            manifest.get("rights") != "not established for Hub hosting or redistribution" or
            manifest.get("network_calls") != 0 or
            manifest.get("data_file") != f"data/{data_name}" or
            manifest.get("source_candidate_sha256") != source_hash or
            manifest.get("data_sha256") != source_hash or
            manifest.get("card_sha256") != hashlib.sha256(card).hexdigest() or
            sha256(target / "data" / data_name) != source_hash or
            sha256(target / "README.md") != manifest["card_sha256"]):
        raise ValueError("existing local package differs from the checked NOAA candidate")
    return manifest


def package_local(
    month: str, *, raw_root: Path = Path("data/raw"),
    processed_root: Path = Path("data/processed"),
    output_root: Path = Path("data/local_dataset"),
) -> dict:
    """Build an ignored three-file local package; never imports/contacts HF."""
    if not MONTH.fullmatch(month):
        raise ValueError("only an approved NOAA 2024 calendar month is supported")
    if output_root.name != "local_dataset" or output_root.parent.name != "data":
        raise ValueError("output must stay in a data/local_dataset directory")
    payload, row, source_meta = _read_candidate(month, processed_root)
    audit = _verify_month(month, raw_root, processed_root, row)
    digest = source_meta["sha256"]
    candidate_file = processed_root / "noaa_candidate" / f"{month}-{REGION}.parquet"
    if sha256(candidate_file) != digest:
        raise ValueError("candidate changed while validating its source")
    card = _render_card(month, audit["calendar_days_verified"])
    output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    target = output_root / f"noaa-{month}-{REGION}"
    data_name = f"noaa_aggregate_{month}.parquet"
    if output_root.is_symlink() or target.is_symlink():
        raise ValueError("local package path must not be a symlink")
    if target.exists():
        result = _existing_package(target, data_name, digest, card, row, audit)
        return {"path": str(target), **result}
    with tempfile.TemporaryDirectory(prefix=f".noaa-{month}-", dir=output_root) as staging:
        stage = Path(staging)
        (stage / "data").mkdir(mode=0o700)
        (stage / "README.md").write_bytes(card)
        (stage / "data" / data_name).write_bytes(payload)
        manifest = {
            "status": "draft_local_not_published", "source": SOURCE,
            "month": month, "region": REGION,
            "source_candidate_sha256": digest,
            "data_file": f"data/{data_name}", "data_sha256": sha256(stage / "data" / data_name),
            "card_sha256": sha256(stage / "README.md"),
            "columns": PUBLIC_SCHEMA.names, "rows": 1,
            "cargo_vessels": row["cargo_vessels"], "observations": row["observations"],
            **audit,
            "rights": "not established for Hub hosting or redistribution",
            "network_calls": 0,
        }
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        for file in (stage / "README.md", stage / "data" / data_name, stage / "manifest.json"):
            file.chmod(0o600)
            with file.open("rb") as handle:
                os.fsync(handle.fileno())
        stage.rename(target)
    return {"path": str(target), **manifest}


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ONLY an ignored local NOAA Dataset Card + Parquet")
    parser.add_argument("month", help="verified NOAA 2024 month YYYY-MM")
    args = parser.parse_args()
    result = package_local(args.month)
    print(json.dumps({k: result[k] for k in ("path", "status", "month", "region", "rows", "cargo_vessels", "observations")}, indent=2))


if __name__ == "__main__":
    main()

"""Téléchargement NOAA CSV.zst par jour, puis conversion locale bornée en JSONL.

Aucune donnée AIS ne doit être versionnée ni publiée. La reprise s'effectue par jour
achevé (manifest + SHA-256), jamais par un décalage au milieu d'un flux zstd.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import random
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import aiohttp
import zstandard

LOG = logging.getLogger(__name__)
PILOT_DATE = date(2024, 1, 14)
PILOT_BOUNDS = (39.0, -75.0, 42.0, -71.0)  # south, west, north, east
MAX_ARCHIVE_BYTES = 1_000_000_000
MAX_OUTPUT_BYTES = 2_000_000_000
CHUNK = 256 * 1024


@dataclass(frozen=True)
class Day:
    day: date
    bounds: tuple[float, float, float, float] = PILOT_BOUNDS
    zone: str = "ny-nj"
    url_override: str | None = None  # injection réservée aux tests locaux

    @property
    def url(self) -> str:
        return self.url_override or (
            f"https://noaaocm.blob.core.windows.net/ais/csv2/csv{self.day.year}/"
            f"ais-{self.day.isoformat()}.csv.zst"
        )


class RetryableDownload(Exception):
    pass


def _column(row: dict[str, str], *aliases: str) -> str | None:
    normalized = {k.lower().replace("_", "").strip(): v for k, v in row.items() if k}
    for alias in aliases:
        result = normalized.get(alias.lower().replace("_", ""))
        if result is not None:
            return result
    return None


def _utc(value: str | None) -> str:
    if not value:
        raise ValueError("missing UTC timestamp")
    dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:  # NOAA documents UTC for its BaseDateTime field
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _convert(archive: Path, output_part: Path, unit: Day) -> dict[str, object]:
    total = selected = invalid = written_bytes = 0
    checksum = hashlib.sha256()
    with archive.open("rb") as compressed, zstandard.ZstdDecompressor().stream_reader(compressed) as stream:
        with io.TextIOWrapper(stream, encoding="utf-8-sig", newline="") as text, output_part.open("wb") as output:
            reader = csv.DictReader(text)
            if not reader.fieldnames or not all(
                any(c.lower().replace("_", "") in aliases for c in reader.fieldnames)
                for aliases in ({"basedatetime"}, {"lat", "latitude"}, {"lon", "longitude"})
            ):
                raise ValueError("NOAA CSV columns missing (base_date_time/latitude/longitude)")
            for row in reader:
                total += 1
                try:
                    lat = float(_column(row, "lat", "latitude") or "nan")
                    lon = float(_column(row, "lon", "longitude") or "nan")
                    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                        raise ValueError("invalid coordinates")
                    if not (unit.bounds[0] <= lat <= unit.bounds[2] and unit.bounds[1] <= lon <= unit.bounds[3]):
                        continue
                    observed_at = _utc(_column(row, "BaseDateTime"))
                    if date.fromisoformat(observed_at[:10]) != unit.day:
                        raise ValueError("wrong UTC date")
                except (ValueError, OverflowError):
                    invalid += 1
                    continue
                record = {
                    "source": "noaa", "source_url": unit.url, "observed_at": observed_at,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "vessel_type": _column(row, "VesselType", "vessel_type"),
                    "record": row,
                }
                line = (json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
                written_bytes += len(line)
                if written_bytes > MAX_OUTPUT_BYTES:
                    raise ValueError("filtered JSONL exceeds output limit")
                output.write(line)
                checksum.update(line)
                selected += 1
            output.flush()
            os.fsync(output.fileno())
    return {"archive_bytes": archive.stat().st_size, "rows_total": total,
            "rows_selected": selected, "rows_invalid": invalid,
            "output_bytes": written_bytes, "sha256": checksum.hexdigest()}


def _is_complete(output: Path, manifest: Path, unit: Day) -> bool:
    if not output.exists() or not manifest.exists():
        return False
    try:
        meta = json.loads(manifest.read_text())
        if meta["url"] != unit.url or meta["zone"] != unit.zone or meta["bounds"] != list(unit.bounds):
            return False
        if meta["output_bytes"] != output.stat().st_size:
            return False
        digest = hashlib.sha256()
        with output.open("rb") as file:
            for chunk in iter(lambda: file.read(CHUNK), b""):
                digest.update(chunk)
        return digest.hexdigest() == meta["sha256"]
    except (OSError, KeyError, ValueError, TypeError):
        return False


async def _download(
    session: aiohttp.ClientSession, url: str, archive_part: Path,
    *, attempts: int = 4, max_bytes: int = MAX_ARCHIVE_BYTES,
) -> None:
    for attempt in range(attempts):
        try:
            async with session.get(url) as response:
                if response.status in (429, 500, 502, 503, 504):
                    raise RetryableDownload(f"HTTP {response.status}")
                response.raise_for_status()  # permanent 4xx -> fail fast
                if response.content_length is not None and response.content_length > max_bytes:
                    raise ValueError("NOAA archive exceeds compressed-size limit")
                size = 0
                with archive_part.open("wb") as file:
                    async for chunk in response.content.iter_chunked(CHUNK):
                        size += len(chunk)
                        if size > max_bytes:
                            raise ValueError("NOAA archive exceeds compressed-size limit")
                        file.write(chunk)
                    file.flush()
                    os.fsync(file.fileno())
                return
        except (aiohttp.ClientError, asyncio.TimeoutError, RetryableDownload) as exc:
            if attempt == attempts - 1:
                raise
            delay = min(30.0, 2**attempt) + random.uniform(0, 0.5)
            LOG.warning("NOAA retry %s/%s after %s; delay %.2fs", attempt + 1, attempts, type(exc).__name__, delay)
            await asyncio.sleep(delay)
    raise RuntimeError("download attempts exhausted")


async def ingest_day(
    unit: Day, *, root: Path = Path("data/raw/noaa"),
    session: aiohttp.ClientSession | None = None, semaphore: asyncio.Semaphore | None = None,
    min_free_bytes: int = 3_000_000_000,
) -> dict[str, object]:
    """Traite un fichier national par jour. N'efface pas le résultat achevé valide."""
    root.mkdir(parents=True, exist_ok=True)
    stem = f"{unit.day.isoformat()}-{unit.zone}"
    output = root / f"{stem}.jsonl"
    manifest = root / f"{stem}.manifest.json"
    archive_part = root / f"{stem}.csv.zst.part"
    output_part = root / f"{stem}.jsonl.part"
    manifest_part = root / f"{stem}.manifest.json.part"
    lock = semaphore or asyncio.Semaphore(1)
    async with lock:
        if _is_complete(output, manifest, unit):
            LOG.info("NOAA completed unit reused: %s", output)
            return json.loads(manifest.read_text())
        # Avoid racing downloads on same destination; caller must submit distinct days.
        if shutil.disk_usage(root).free < min_free_bytes:
            raise OSError(f"insufficient free disk space: need {min_free_bytes} bytes")
        for partial in (archive_part, output_part, manifest_part):
            partial.unlink(missing_ok=True)
        owns_session = session is None
        if owns_session:
            timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=60)
            session = aiohttp.ClientSession(timeout=timeout)
        try:
            assert session is not None
            await _download(session, unit.url, archive_part)
            stats = await asyncio.to_thread(_convert, archive_part, output_part, unit)
            metadata = {"source": "noaa", "date": unit.day.isoformat(), "zone": unit.zone,
                        "bounds": list(unit.bounds), "url": unit.url, **stats,
                        "completed_at": datetime.now(timezone.utc).isoformat()}
            # Write/checkpoint atomically. On a crash between the two renames, next run
            # discards the incomplete checkpoint and rebuilds rather than appending.
            manifest_part.write_text(json.dumps(metadata, indent=2) + "\n")
            with manifest_part.open("rb") as file:
                os.fsync(file.fileno())
            output_part.replace(output)
            manifest_part.replace(manifest)
            LOG.info("NOAA %s: %s / %s rows selected, %s bytes", unit.day, stats["rows_selected"], stats["rows_total"], stats["output_bytes"])
            return metadata
        finally:
            if owns_session:
                await session.close()
            archive_part.unlink(missing_ok=True)
            output_part.unlink(missing_ok=True)
            manifest_part.unlink(missing_ok=True)


async def ingest_days(days: list[Day], *, root: Path = Path("data/raw/noaa"), concurrency: int = 2) -> list[dict[str, object]]:
    if not 1 <= concurrency <= 3:
        raise ValueError("concurrency must be between 1 and 3")
    if len({(u.day, u.zone) for u in days}) != len(days):
        raise ValueError("duplicate day/zone output")
    semaphore = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        return await asyncio.gather(*(ingest_day(u, root=root, session=session, semaphore=semaphore) for u in days))


def main() -> None:
    parser = argparse.ArgumentParser(description="Local-only NOAA daily CSV.zst ingestion")
    parser.add_argument("--date", type=date.fromisoformat, default=PILOT_DATE)
    parser.add_argument("--root", type=Path, default=Path("data/raw/noaa"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    asyncio.run(ingest_days([Day(args.date)], root=args.root))


if __name__ == "__main__":
    main()

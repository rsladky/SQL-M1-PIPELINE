"""Capture AISStream locale et bornée. Pas d'historique/rejeu après déconnexion."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import random
import re
import shutil
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp

LOG = logging.getLogger(__name__)
ENDPOINT = "wss://stream.aisstream.io/v0/stream"
# AISStream bounding box order: [north/west, south/east].
NY_NJ = [[[42.0, -75.0], [39.0, -71.0]]]
POSITION_TYPES = {"PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"}
STATIC_TYPES = {"ShipStaticData", "StaticDataReport"}
MESSAGE_TYPES = sorted(POSITION_TYPES | STATIC_TYPES)
MAX_CACHE = 50_000
MAX_SEEN = 20_000


def _save_json(path: Path, value: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as file:
        json.dump(value, file)
        file.flush()
        os.fsync(file.fileno())
    tmp.replace(path)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Capture:
    def __init__(self, root: Path, session_id: str, *, duration: int = 900, queue_size: int = 1000):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", session_id):
            raise ValueError("unsafe session id")
        if duration < 1 or duration > 900 or queue_size < 1:
            raise ValueError("duration must be 1..900 seconds and queue must be bounded")
        self.root = root
        self.session_id = session_id
        self.duration = duration
        self.queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=queue_size)
        self.checkpoint = root / f"{session_id}.checkpoint.json"
        self.part = root / f"{session_id}.jsonl.part"
        self.output = root / f"{session_id}.jsonl"
        self.manifest = root / f"{session_id}.manifest.json"
        self.types: OrderedDict[str, int] = OrderedDict()
        self.seen: OrderedDict[str, None] = OrderedDict()
        self.rows = 0
        self.dropped = 0
        self.interruptions = 0
        self.end_at: datetime | None = None

    def _restore(self) -> bool:
        self.root.mkdir(parents=True, exist_ok=True)
        if self.output.exists() and self.manifest.exists():
            LOG.info("Finished session %s already exists; no new capture", self.session_id)
            return True
        if self.checkpoint.exists():
            state = json.loads(self.checkpoint.read_text())
            self.end_at = datetime.fromisoformat(state["end_at"])
            self.interruptions = state["interruptions"]
            self.dropped = state["dropped"]
            self.types = OrderedDict((k, int(v)) for k, v in state.get("types", []))
            if not self.part.exists():
                raise RuntimeError("checkpoint exists but session JSONL is missing")
        else:
            if self.part.exists() or self.output.exists():
                raise RuntimeError("partial session without checkpoint; inspect manually")
            self.end_at = _utcnow() + timedelta(seconds=self.duration)
            self.part.touch()
        # Scan committed complete lines rather than trusting a possibly stale checkpoint.
        # Deduplication is intentionally limited to the last MAX_SEEN observations.
        with self.part.open("rb+") as file:
            while line := file.readline():
                if not line.endswith(b"\n"):
                    file.truncate(file.tell() - len(line))
                    break
                record = json.loads(line)
                self.rows += 1
                key = record["dedup_key"]
                self._bounded(self.seen, key, None, MAX_SEEN)
        self._checkpoint()
        return False

    @staticmethod
    def _bounded(cache: OrderedDict, key: str, value: object, limit: int) -> None:
        cache[key] = value
        cache.move_to_end(key)
        if len(cache) > limit:
            cache.popitem(last=False)

    def _checkpoint(self) -> None:
        assert self.end_at is not None
        _save_json(self.checkpoint, {
            "end_at": self.end_at.isoformat(), "rows": self.rows,
            "interruptions": self.interruptions, "dropped": self.dropped,
            "types": list(self.types.items()), "last_flush_at": _utcnow().isoformat(),
        })

    def accept(self, message: dict) -> dict | None:
        kind = message.get("MessageType")
        metadata = message.get("MetaData") or {}
        mmsi = str(metadata.get("MMSI") or "")
        if not mmsi or kind not in MESSAGE_TYPES:
            return None
        payload = (message.get("Message") or {}).get(kind) or {}
        if kind in STATIC_TYPES:
            # Static messages carry vessel classification; positions alone cannot establish cargo.
            value = payload.get("Type", payload.get("ShipType", payload.get("VesselType")))
            try:
                vessel_type = int(value)
            except (ValueError, TypeError):
                return None
            self._bounded(self.types, mmsi, vessel_type, MAX_CACHE)
            return None
        vessel_type = self.types.get(mmsi)
        if vessel_type not in range(70, 80):
            return None
        self.types.move_to_end(mmsi)
        # Hash canonical payload to suppress repeated frames after reconnect/restart. The
        # rolling window is bounded; it is not a promise of full-stream deduplication.
        digest = hashlib.sha256(json.dumps(message, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if digest in self.seen:
            return None
        self._bounded(self.seen, digest, None, MAX_SEEN)
        return {"source": "aisstream", "vessel_type": vessel_type,
                "received_at": _utcnow().isoformat(), "dedup_key": digest, "record": message}

    async def _writer(self) -> None:
        with self.part.open("ab") as file:
            try:
                while True:
                    record = await self.queue.get()
                    try:
                        file.write((json.dumps(record, separators=(",", ":")) + "\n").encode())
                        self.rows += 1
                        if self.rows % 100 == 0:
                            file.flush()
                            os.fsync(file.fileno())
                            self._checkpoint()
                    finally:
                        self.queue.task_done()
            finally:
                # Cancellation after queue.join() must persist the final (<100) batch.
                file.flush()
                os.fsync(file.fileno())

    async def run(self, *, api_key: str | None = None, session: aiohttp.ClientSession | None = None) -> dict:
        api_key = api_key or os.environ.get("AISSTREAM_API_KEY")
        if not api_key:
            raise RuntimeError("AISSTREAM_API_KEY missing; refusing to contact AISStream")
        if shutil.disk_usage(self.root if self.root.exists() else self.root.parent).free < 100_000_000:
            raise OSError("insufficient free disk space for AISStream capture (100 MB)")
        if self._restore():
            return json.loads(self.manifest.read_text())
        assert self.end_at is not None
        writer = asyncio.create_task(self._writer())
        owns_session = session is None
        if owns_session:
            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, connect=10))
        try:
            backoff = 0
            while (remaining := (self.end_at - _utcnow()).total_seconds()) > 0:
                try:
                    assert session is not None
                    async with session.ws_connect(ENDPOINT, compress=15, heartbeat=20, timeout=aiohttp.ClientWSTimeout(ws_receive=30)) as ws:
                        # Full subscription must be sent within 3 seconds of connection.
                        await ws.send_json({"APIKey": api_key, "BoundingBoxes": NY_NJ,
                                            "FilterMessageTypes": MESSAGE_TYPES})
                        backoff = 0
                        while (remaining := (self.end_at - _utcnow()).total_seconds()) > 0:
                            try:
                                frame = await asyncio.wait_for(ws.receive(), timeout=min(30, remaining))
                            except asyncio.TimeoutError:
                                if (_utcnow() >= self.end_at):
                                    break
                                raise ConnectionError("no AISStream messages for 30s")
                            if frame.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                                raise ConnectionError(f"AISStream disconnected: {frame.type.name}")
                            if frame.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                                continue
                            raw = frame.data.decode("utf-8") if isinstance(frame.data, bytes) else frame.data
                            message = json.loads(raw)
                            if message.get("MessageType") == "SubscriptionConfirmation":
                                LOG.info("subscription confirmed; compression=%s", message.get("Message", {}).get("CompressionEnabled"))
                                continue
                            record = self.accept(message)
                            if record is not None:
                                try:
                                    self.queue.put_nowait(record)
                                except asyncio.QueueFull:
                                    self.dropped += 1
                                    LOG.warning("AISStream local queue full: record dropped")
                except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError, OSError, ValueError, json.JSONDecodeError) as exc:
                    self.interruptions += 1
                    LOG.warning("AISStream interruption %d: %s (lost messages cannot be replayed)", self.interruptions, type(exc).__name__)
                    self._checkpoint()
                    backoff = min(30, backoff + 1)
                    await asyncio.sleep(min((self.end_at - _utcnow()).total_seconds(), 2**min(backoff, 4) + random.random()))
            await self.queue.join()
        finally:
            await self.queue.join()
            writer.cancel()
            try:
                await writer
            except asyncio.CancelledError:
                pass
            if owns_session:
                await session.close()
            # Flush the writer's context manager on cancellation and checkpoint progress.
            self._checkpoint()
        self.part.replace(self.output)
        metadata = {"source": "aisstream", "session_id": self.session_id,
                    "rows": self.rows, "dropped": self.dropped,
                    "interruptions": self.interruptions, "ended_at": _utcnow().isoformat(),
                    "warning": "Live stream; reconnects do not recover lost messages"}
        _save_json(self.manifest, metadata)
        self.checkpoint.unlink(missing_ok=True)
        return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description="Local-only AISStream capture (requires key)")
    parser.add_argument("--session-id", help="Use same ID to resume a stopped capture")
    parser.add_argument("--duration", type=int, default=900, help="Seconds; max 900")
    parser.add_argument("--root", type=Path, default=Path("data/raw/aisstream"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    session_id = args.session_id or _utcnow().strftime("%Y%m%dT%H%M%SZ")
    print(asyncio.run(Capture(args.root, session_id, duration=args.duration).run()))


if __name__ == "__main__":
    main()

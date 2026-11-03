"""Fixtures AIS synthétiques uniquement ; aucune donnée réelle committée."""

import asyncio
import hashlib
import json
from datetime import date

import aiohttp
from aiohttp import web
import pytest
import zstandard

from scripts.ingest import noaa


CSV = (
    "mmsi,base_date_time,latitude,longitude,vessel_type,vessel_name\n"
    "100000001,2024-01-14T12:34:56,40.1,-73.2,70,TEST\n"
    "100000002,2024-01-14T12:35:00,21.1,-73.2,75,OUTSIDE\n"
    "100000003,2024-01-14T12:36:00,91.1,-73.2,70,BAD\n"
    "100000004,2024-01-15T12:36:00,40.5,-73.2,70,WRONG_DAY\n"
).encode()
ARCHIVE = zstandard.ZstdCompressor().compress(CSV)


async def serve(handler):
    app = web.Application()
    app.router.add_get("/pilot.csv.zst", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/pilot.csv.zst"
    return runner, url


def test_noaa_429_retry_and_resume(tmp_path, monkeypatch):
    async def scenario():
        calls = 0
        async def handler(request):
            nonlocal calls
            calls += 1
            if calls == 1:
                return web.Response(status=429)
            if calls == 2:
                return web.Response(status=503)
            return web.Response(body=ARCHIVE)
        runner, url = await serve(handler)
        try:
            async def no_delay(_):
                pass
            monkeypatch.setattr(noaa.asyncio, "sleep", no_delay)
            unit = noaa.Day(date(2024, 1, 14), url_override=url)
            result = await noaa.ingest_day(unit, root=tmp_path, min_free_bytes=1)
            assert calls == 3
            assert (result["rows_total"], result["rows_selected"], result["rows_invalid"]) == (4, 1, 2)
            output = tmp_path / "2024-01-14-ny-nj.jsonl"
            rows = [json.loads(line) for line in output.read_text().splitlines()]
            assert len(rows) == 1
            assert rows[0]["source"] == "noaa" and rows[0]["source_url"] == url
            assert rows[0]["observed_at"] == "2024-01-14T12:34:56Z"
            assert result["sha256"] == hashlib.sha256(output.read_bytes()).hexdigest()
            assert await noaa.ingest_day(unit, root=tmp_path, min_free_bytes=1) == result
            assert calls == 3  # manifest and checksum permit a no-network restart
            output.write_text("corrupt")
            result2 = await noaa.ingest_day(unit, root=tmp_path, min_free_bytes=1)
            assert calls == 4 and result2["rows_selected"] == 1
            assert not list(tmp_path.glob("*.part"))
        finally:
            await runner.cleanup()
    asyncio.run(scenario())


def test_noaa_timeout_and_partial_cleanup(tmp_path, monkeypatch):
    async def scenario():
        original_sleep = asyncio.sleep
        async def handler(request):
            await original_sleep(.2)
            return web.Response(body=ARCHIVE)
        runner, url = await serve(handler)
        try:
            async def no_delay(_):
                return None
            monkeypatch.setattr(noaa.asyncio, "sleep", no_delay)
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=.03)) as session:
                with pytest.raises(asyncio.TimeoutError):
                    await noaa.ingest_day(noaa.Day(date(2024, 1, 14), url_override=url),
                                          root=tmp_path, session=session, min_free_bytes=1)
            assert not list(tmp_path.glob("*.part"))
            assert not list(tmp_path.glob("*.jsonl"))
        finally:
            monkeypatch.setattr(noaa.asyncio, "sleep", original_sleep)
            await runner.cleanup()
    asyncio.run(scenario())


def test_noaa_disk_check_and_duplicate_units(tmp_path):
    async def scenario():
        with pytest.raises(OSError, match="disk"):
            await noaa.ingest_day(noaa.Day(date(2024, 1, 14)), root=tmp_path, min_free_bytes=10**30)
        with pytest.raises(ValueError, match="duplicate"):
            await noaa.ingest_days([noaa.Day(date(2024, 1, 14))] * 2, root=tmp_path)
    asyncio.run(scenario())

import asyncio
import json
from datetime import timedelta

import aiohttp
import pytest

from scripts.ingest import aisstream


STATIC = {"MessageType": "ShipStaticData", "MetaData": {"MMSI": 100000001},
          "Message": {"ShipStaticData": {"Type": 70}}}
POSITION = {"MessageType": "PositionReport", "MetaData": {"MMSI": 100000001, "Latitude": 40.2},
            "Message": {"PositionReport": {"Sog": 8}}}


def test_cargo_requires_static_and_deduplicates(tmp_path):
    capture = aisstream.Capture(tmp_path, "test1", duration=1, queue_size=1)
    assert capture.accept(POSITION) is None  # no cargo classification yet
    assert capture.accept(STATIC) is None
    record = capture.accept(POSITION)
    assert record["source"] == "aisstream" and record["vessel_type"] == 70
    assert capture.accept(POSITION) is None
    assert capture.accept({**STATIC, "Message": {"ShipStaticData": {"Type": 80}}}) is None
    assert capture.accept({**POSITION, "Message": {"PositionReport": {"Sog": 9}}}) is None
    with pytest.raises(ValueError, match="session id"):
        aisstream.Capture(tmp_path, "../unsafe")
    with pytest.raises(ValueError, match="duration"):
        aisstream.Capture(tmp_path, "long", duration=901)


def test_missing_key_does_not_create_files_or_connect(tmp_path, monkeypatch):
    monkeypatch.delenv("AISSTREAM_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="AISSTREAM_API_KEY"):
        asyncio.run(aisstream.Capture(tmp_path, "none", duration=1).run())
    assert not list(tmp_path.iterdir())


def test_resume_truncates_incomplete_line_and_retains_dedup(tmp_path):
    capture = aisstream.Capture(tmp_path, "resume", duration=1)
    assert capture._restore() is False
    capture.accept(STATIC)
    record = capture.accept(POSITION)
    capture.part.write_bytes((json.dumps(record) + "\n").encode() + b'{"broken":')
    capture._checkpoint()
    next_capture = aisstream.Capture(tmp_path, "resume", duration=1)
    assert next_capture._restore() is False
    assert next_capture.rows == 1
    assert next_capture.part.read_bytes().endswith(b"\n")
    assert next_capture.accept(POSITION) is None


class FakeWS:
    def __init__(self):
        self.messages = [
            aiohttp.WSMessage(aiohttp.WSMsgType.BINARY, json.dumps(STATIC).encode(), ""),
            aiohttp.WSMessage(aiohttp.WSMsgType.BINARY, json.dumps(POSITION).encode(), ""),
            aiohttp.WSMessage(aiohttp.WSMsgType.CLOSED, None, ""),
        ]
        self.sent = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def send_json(self, data):
        self.sent = data

    async def receive(self):
        if self.messages:
            return self.messages.pop(0)
        return aiohttp.WSMessage(aiohttp.WSMsgType.CLOSED, None, "")


class FakeSession:
    def __init__(self):
        self.calls = []

    def ws_connect(self, url, **kwargs):
        ws = FakeWS()
        self.calls.append((url, kwargs, ws))
        return ws


def test_interruption_and_session_checkpoint(tmp_path):
    async def scenario():
        session = FakeSession()
        capture = aisstream.Capture(tmp_path, "test-run", duration=1)
        result = await capture.run(api_key="SYNTHETIC_TEST_KEY", session=session)
        assert result["rows"] == 1 and result["interruptions"] >= 1
        assert len(session.calls) >= 1
        assert session.calls[0][2].sent["APIKey"] == "SYNTHETIC_TEST_KEY"
        assert session.calls[0][1]["compress"] == 15
        assert "SYNTHETIC_TEST_KEY" not in (tmp_path / "test-run.jsonl").read_text()
        assert "SYNTHETIC_TEST_KEY" not in (tmp_path / "test-run.manifest.json").read_text()
        assert not capture.checkpoint.exists()
        assert len((tmp_path / "test-run.jsonl").read_text().splitlines()) == 1
        assert await aisstream.Capture(tmp_path, "test-run", duration=1).run(api_key="KEY", session=session) == result
    asyncio.run(scenario())

"""Zero network calls / tokens; every publication fixture is synthetic."""

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.publish.private_hub import (
    CONFIRMATION, PRODUCT, Ready, preflight, upload_private,
)
from scripts.transform.aggregate import candidate
from scripts.transform.batches import sha256, transform
from test_transform import noaa_input, noaa_row

REPO = "synthetic/noaa-aggregate-test"
CITATION = "NOAA Office for Coastal Management. (2024). AIS Broadcast Points. Marine Cadastre. https://marinecadastre.gov"
NOTICE = "For synthetic test fixtures only: written NOAA permission and all restrictions reviewed for a private derived-product upload."


def _complete_month(tmp_path):
    raw_root, processed = tmp_path / "data/raw", tmp_path / "data/processed"
    first = date(2024, 1, 1)
    for offset in range(31):
        day = (first + timedelta(days=offset)).isoformat()
        rows = []
        if offset < 7:
            for vessel in range(20):
                rows.extend([noaa_row(day, mmsi=100000001 + vessel, hour=12),
                             noaa_row(day, mmsi=100000001 + vessel, hour=13, lat=40.5001)])
        transform(noaa_input(raw_root, day, rows), root=processed)
    result = candidate("2024-01", raw_root=raw_root, processed_root=processed)
    assert result["status"] == "candidate_not_approved_for_publication"
    return raw_root, processed


def _approval(tmp_path, processed, *, repo=REPO, status="confirmed_written_noaa_permission"):
    directory = tmp_path / "data/approvals"
    directory.mkdir(parents=True, exist_ok=True)
    evidence = directory / "synthetic-permission.txt"
    evidence.write_text("SYNTHETIC TEST DOCUMENT ONLY. Not a real NOAA permission or authorization.")
    candidate_path = processed / "noaa_candidate/2024-01-ny-nj.parquet"
    review = {
        "rights_status": status, "private_upload_authorized": True,
        "privacy_review": "passed", "reviewer": "Synthetic Test Reviewer",
        "reviewed_on": date.today().isoformat(), "month": "2024-01",
        "region": "ny-nj", "candidate_sha256": sha256(candidate_path),
        "repo_id": repo, "source_product": PRODUCT,
        "evidence_file": evidence.name, "evidence_sha256": sha256(evidence),
        "citation": CITATION, "conditions_summary": NOTICE,
    }
    approval = directory / "noaa-hub.json"
    approval.write_text(json.dumps(review))
    return approval


class FakeHub:
    def __init__(self, *, private=True, files=None):
        self.private = private
        self.files = [".gitattributes"] if files is None else files
        self.calls = []

    def repo_info(self, *, repo_id, repo_type, token):
        self.calls.append("repo_info")
        assert repo_type == "dataset" and token == "SYNTHETIC_FAKE_TOKEN"
        return SimpleNamespace(private=self.private, id=repo_id)

    def list_repo_files(self, *, repo_id, repo_type, token):
        self.calls.append("list_repo_files")
        return self.files

    def create_commit(self, *, repo_id, repo_type, token, commit_message, operations):
        self.calls.append("create_commit")
        paths = [op.path_in_repo for op in operations]
        assert paths == ["README.md", "data/noaa_aggregate_2024-01.parquet"]
        card, data = [op.path_or_fileobj for op in operations]
        assert isinstance(card, bytes) and isinstance(data, bytes)
        assert b"2024-01" in card and b"SYNTHETIC TEST DOCUMENT" not in card
        assert b"aisstream" not in data.lower()
        assert b"mmsi" not in data.lower()
        assert b"longitude" not in data.lower()
        assert b"hf_token" not in card.lower()
        assert b"SYNTHETIC_FAKE_TOKEN" not in data + card


def test_missing_candidate_offline_no_hub(tmp_path, monkeypatch):
    # Never depend on whether the operator has collected the real NOAA month.
    import scripts.publish.private_hub as module
    monkeypatch.setattr(module, "HfApi", lambda: (_ for _ in ()).throw(AssertionError("network attempted")))
    raw = tmp_path / "data/raw"
    processed = tmp_path / "data/processed"
    approval = tmp_path / "data/approvals/noaa-hub.json"
    with pytest.raises(ValueError, match="candidate absent"):
        preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)
    with pytest.raises(ValueError, match="2024 NOAA month"):
        preflight("2025-01", REPO, approval, raw_root=raw, processed_root=processed)


def test_rights_not_confirmed_and_missing_token_block_before_hub(tmp_path, monkeypatch):
    raw, processed = _complete_month(tmp_path)
    approval = _approval(tmp_path, processed, status="pending")
    with pytest.raises(PermissionError, match="not confirmed"):
        preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)
    approval = _approval(tmp_path, processed)
    ready = preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)
    assert isinstance(ready, Ready)
    assert b"No AISStream data" in ready.card and b"License:" in ready.card
    assert b"license: unknown" not in ready.card and b"license: other" not in ready.card
    fake = FakeHub()
    monkeypatch.delenv("HF_TOKEN", raising=False)
    with pytest.raises(PermissionError, match="confirmation"):
        upload_private(ready, approval_path=approval, raw_root=raw, processed_root=processed, api=fake)
    with pytest.raises(PermissionError, match="HF_TOKEN"):
        upload_private(ready, approval_path=approval, raw_root=raw, processed_root=processed,
                       api=fake, confirmation=CONFIRMATION)
    assert not fake.calls


def test_public_or_nonempty_remote_repo_rejected(tmp_path):
    raw, processed = _complete_month(tmp_path)
    approval = _approval(tmp_path, processed)
    ready = preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)
    for fake in (FakeHub(private=False), FakeHub(files=["README.md", "data/aisstream.jsonl"])):
        with pytest.raises(PermissionError):
            upload_private(ready, approval_path=approval, raw_root=raw, processed_root=processed,
                           api=fake, token="SYNTHETIC_FAKE_TOKEN", confirmation=CONFIRMATION)
        assert "create_commit" not in fake.calls


def test_only_card_and_approved_noaa_bytes_can_be_sent_to_mock(tmp_path):
    raw, processed = _complete_month(tmp_path)
    # An unrelated local AISStream file must never enter the upload operations.
    private_ais = raw / "aisstream" / "synthetic.jsonl"
    private_ais.parent.mkdir()
    private_ais.write_text('{"source":"aisstream","MMSI":"100000001"}\n')
    approval = _approval(tmp_path, processed)
    ready = preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)
    fake = FakeHub()
    assert upload_private(ready, approval_path=approval, raw_root=raw, processed_root=processed,
                          api=fake, token="SYNTHETIC_FAKE_TOKEN", confirmation=CONFIRMATION) == (
                          "https://huggingface.co/datasets/" + REPO)
    assert fake.calls == ["repo_info", "list_repo_files", "create_commit", "repo_info"]


def test_tamper_missing_days_forged_columns_or_changed_approval_block(tmp_path):
    raw, processed = _complete_month(tmp_path)
    approval = _approval(tmp_path, processed)
    ready = preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)
    # Approval for a different hash may not upload even if the local Parquet exists.
    review = json.loads(approval.read_text())
    review["candidate_sha256"] = "0" * 64
    approval.write_text(json.dumps(review))
    with pytest.raises(PermissionError, match="not confirmed"):
        preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)
    approval = _approval(tmp_path, processed)
    # A fabricated extra identifier column and matching forged manifest cannot pass
    # full-month regeneration, even if someone passes a stale Ready object.
    file = processed / "noaa_candidate/2024-01-ny-nj.parquet"
    table = pq.read_table(file).append_column("mmsi", pa.array(["100000001"]))
    pq.write_table(table, file)
    manifest = file.with_suffix(".manifest.json")
    meta = json.loads(manifest.read_text())
    meta["sha256"] = sha256(file)
    manifest.write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="candidate changed"):
        upload_private(ready, approval_path=approval, raw_root=raw, processed_root=processed,
                       api=FakeHub(), token="SYNTHETIC_FAKE_TOKEN", confirmation=CONFIRMATION)
    # Even if a prior candidate existed, missing a daily ingestion unit suppresses it.
    (raw / "noaa/2024-01-31-ny-nj.manifest.json").unlink()
    with pytest.raises(ValueError, match="candidate absent|month suppressed"):
        preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)

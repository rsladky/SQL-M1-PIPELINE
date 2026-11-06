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
    CONFIRMATION, FAQ_URL, OWNER_ATTESTATION, PRODUCT, PRODUCT_INDEX,
    REVIEW_STATUS, Ready, preflight, upload_private,
)
from scripts.transform.aggregate import candidate
from scripts.transform.batches import sha256, transform
from test_transform import noaa_input, noaa_row

REPO = "synthetic/noaa-aggregate-test"
CITATION = f"NOAA Office for Coastal Management. (2024). AIS Broadcast Points, 2024 daily national CSV.zst archives. Marine Cadastre. {PRODUCT_INDEX}"
NOTICE = "Private upload interpretation is limited to this aggregate and intended coastal planning use; it is not an individual NOAA authorization."


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


def _approval(tmp_path, processed, *, repo=REPO, status=REVIEW_STATUS):
    """Fixture ONLY: no real NOAA permission, source review or Hub connection."""
    directory = tmp_path / "data/approvals"
    directory.mkdir(parents=True, exist_ok=True)
    evidence = directory / "synthetic-source-review.md"
    candidate_path = processed / "noaa_candidate/2024-01-ny-nj.parquet"
    digest = sha256(candidate_path)
    # Synthetic fixture only: 73064's exact linkage to csv2 CSV.zst is NOT established.
    notice_url = "https://www.fisheries.noaa.gov/inport/item/73064"
    evidence.write_text(
        f"Synthetic test fixture only; never use for an actual upload.\n"
        f"FAQ {FAQ_URL}; source {PRODUCT_INDEX}; notice {notice_url}.\n"
        f"Candidate hash: {digest}; planned repo: {repo}.\n"
        "The test review records a derived-product FAQ reading alongside USCG AIS-sharing restrictions. "
        "No public release is authorized; the precise product notice must be checked by a human. "
        "Purpose is coastal planning and privacy review includes unique contributors, linkage, "
        "differencing and residual risk. This test document is deliberately not a legal conclusion.\n",
        encoding="utf-8",
    )
    review = {
        "rights_status": status, "private_upload_authorized": True,
        "privacy_review": "passed_with_residual_risk", "reviewer": "Fixture Test Reviewer",
        "reviewed_on": date.today().isoformat(), "month": "2024-01",
        "region": "ny-nj", "candidate_sha256": digest,
        "repo_id": repo, "repo_owner_attestation": OWNER_ATTESTATION,
        "source_product": PRODUCT, "source_product_url": PRODUCT_INDEX,
        "faq_url": FAQ_URL, "faq_revision": "FAQ edition as read in fixture",
        "faq_reviewed_on": date.today().isoformat(),
        "product_notice_url": notice_url,
        "product_notice_matches_csv2024": True,  # only exercises a simulated human claim
        "product_notice_revision": "InPort metadata edition as read in fixture",
        "product_notice_reviewed_on": date.today().isoformat(),
        "evidence_file": evidence.name, "evidence_sha256": sha256(evidence),
        "citation": CITATION, "conditions_summary": NOTICE,
        "derived_product_analysis": "The FAQ discusses derived products with citation, not a blanket AIS retransmission right.",
        "uscg_restriction_analysis": "USCG restrictions on AIS redistribution may limit scope; this private aggregate is not raw AIS.",
        "purpose_compatibility_analysis": "Coastal planning only; no surveillance, enforcement, navigation or detailed AIS retransmission.",
        "privacy_risk_analysis": "The reviewer checked contribution, external linkage and differencing across future releases.",
        "residual_uncertainty": "Legal interpretation and reidentification risk remain uncertain despite monthly aggregation.",
    }
    approval = directory / "noaa-hub.json"
    approval.write_text(json.dumps(review), encoding="utf-8")
    return approval


class FakeHub:
    def __init__(self, *, private=True, files=None, reported_id=None):
        self.private = private
        self.files = [".gitattributes"] if files is None else files
        self.reported_id = reported_id
        self.calls = []

    def repo_info(self, *, repo_id, repo_type, token):
        self.calls.append("repo_info")
        assert repo_type == "dataset" and token == "SYNTHETIC_FAKE_TOKEN"
        return SimpleNamespace(private=self.private, id=self.reported_id or repo_id)

    def list_repo_files(self, *, repo_id, repo_type, token):
        self.calls.append("list_repo_files")
        return self.files

    def create_commit(self, *, repo_id, repo_type, token, commit_message, operations):
        self.calls.append("create_commit")
        paths = [op.path_in_repo for op in operations]
        assert paths == ["README.md", "data/noaa_aggregate_2024-01.parquet"]
        card, data = [op.path_or_fileobj for op in operations]
        assert isinstance(card, bytes) and isinstance(data, bytes)
        assert b"2024-01" in card and b"Synthetic test fixture only" not in card
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


def test_documented_review_and_missing_token_block_before_hub(tmp_path, monkeypatch):
    raw, processed = _complete_month(tmp_path)
    approval = _approval(tmp_path, processed, status="pending")
    with pytest.raises(PermissionError, match="not confirmed"):
        preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)
    approval = _approval(tmp_path, processed)
    ready = preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)
    assert isinstance(ready, Ready)
    assert b"No AISStream data" in ready.card and b"License:" in ready.card
    assert b"license: unknown" not in ready.card and b"license: other" not in ready.card
    assert b"NOAA permission confirmed" not in ready.card
    assert b"not an individual NOAA authorization" in ready.card
    assert b"CSV.zst CC0" in ready.card and b"no CSV.zst CC0 or other open license is asserted" in ready.card
    assert FAQ_URL.encode() in ready.card and b"InPort metadata edition as read in fixture" in ready.card
    assert b"{{" not in ready.card and b"}}" not in ready.card
    assert b"Fixture Test Reviewer" not in ready.card  # personal reviewer identity stays local
    assert b"synthetic-source-review.md" not in ready.card
    fake = FakeHub()
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.setattr("scripts.publish.private_hub.get_token", lambda: None)
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
    for fake in (FakeHub(private=False), FakeHub(reported_id="other/wrong-repo"),
                 FakeHub(files=["README.md", "data/aisstream.jsonl"])):
        with pytest.raises(PermissionError):
            upload_private(ready, approval_path=approval, raw_root=raw, processed_root=processed,
                           api=fake, token="SYNTHETIC_FAKE_TOKEN", confirmation=CONFIRMATION)
        assert "create_commit" not in fake.calls


def test_only_card_and_approved_noaa_bytes_can_be_sent_to_mock(tmp_path, monkeypatch):
    raw, processed = _complete_month(tmp_path)
    # An unrelated local AISStream file must never enter the upload operations.
    private_ais = raw / "aisstream" / "synthetic.jsonl"
    private_ais.parent.mkdir()
    private_ais.write_text('{"source":"aisstream","MMSI":"100000001"}\n')
    approval = _approval(tmp_path, processed)
    ready = preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)
    fake = FakeHub()
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.setattr("scripts.publish.private_hub.get_token", lambda: "SYNTHETIC_FAKE_TOKEN")
    assert upload_private(ready, approval_path=approval, raw_root=raw, processed_root=processed,
                          api=fake, confirmation=CONFIRMATION) == (
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


@pytest.mark.parametrize(("field", "invalid"), [
    ("rights_status", "confirmed_written_noaa_permission"),
    ("private_upload_authorized", False),
    ("repo_owner_attestation", "I_DO_NOT_CONTROL_THE_REPO"),
    ("privacy_review", "passed"),
    ("faq_url", "https://example.invalid/ais/faq.pdf"),
    ("product_notice_url", "https://example.invalid/ais-2024"),
    ("product_notice_url", "https://www.fisheries.noaa.gov/inport/item/55360"),
    ("product_notice_url", "https://www.fisheries.noaa.gov/inport/item/55360/"),
    ("product_notice_url", "https://www.fisheries.noaa.gov/inport/item/%355360"),
    ("product_notice_matches_csv2024", False),
    ("source_product_url", "https://example.invalid/ais-2024"),
    ("faq_revision", ""),
    ("derived_product_analysis", ""),
    ("uscg_restriction_analysis", ""),
    ("purpose_compatibility_analysis", ""),
    ("privacy_risk_analysis", ""),
    ("residual_uncertainty", ""),
    ("citation", "NOAA permission confirmed for all CSV releases"),
    ("conditions_summary", "NOAA permission confirmed for a private derived-product upload"),
    ("evidence_file", "../outside.md"),
    ("candidate_sha256", "0" * 64),
    ("reviewed_on", "2030-01-01"),
    ("faq_reviewed_on", "2020-01-01"),
    ("product_notice_reviewed_on", "2030-01-01"),
])
def test_blank_forged_or_unscoped_attestation_is_rejected(tmp_path, field, invalid):
    raw, processed = _complete_month(tmp_path)
    approval = _approval(tmp_path, processed)
    review = json.loads(approval.read_text())
    review[field] = invalid
    approval.write_text(json.dumps(review))
    with pytest.raises(PermissionError):
        preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)


def test_notes_missing_stale_or_wrong_repo_are_rejected(tmp_path):
    raw, processed = _complete_month(tmp_path)
    approval = _approval(tmp_path, processed)
    review = json.loads(approval.read_text())
    notes = approval.parent / review["evidence_file"]
    notes.write_text("Fake or empty notes cannot substantiate official source and artifact links.")
    with pytest.raises(PermissionError, match="notes"):
        preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)
    approval = _approval(tmp_path, processed)
    with pytest.raises(PermissionError, match="not confirmed"):
        preflight("2024-01", "different/repo", approval,
                  raw_root=raw, processed_root=processed)


def test_reviewer_or_evidence_changes_after_preflight_block_upload(tmp_path):
    raw, processed = _complete_month(tmp_path)
    approval = _approval(tmp_path, processed)
    ready = preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)
    review = json.loads(approval.read_text())
    review["reviewer"] = "Changed Fixture Reviewer"  # card content is unchanged
    approval.write_text(json.dumps(review))
    fake = FakeHub()
    with pytest.raises(ValueError, match="changed since preflight"):
        upload_private(ready, approval_path=approval, raw_root=raw, processed_root=processed,
                       api=fake, token="SYNTHETIC_FAKE_TOKEN", confirmation=CONFIRMATION)
    assert not fake.calls
    ready = preflight("2024-01", REPO, approval, raw_root=raw, processed_root=processed)
    notes = approval.parent / review["evidence_file"]
    notes.write_text(notes.read_text() + "\nMore synthetic test notes, no real authorization.\n")
    review["evidence_sha256"] = sha256(notes)
    approval.write_text(json.dumps(review))
    with pytest.raises(ValueError, match="changed since preflight"):
        upload_private(ready, approval_path=approval, raw_root=raw, processed_root=processed,
                       api=fake, token="SYNTHETIC_FAKE_TOKEN", confirmation=CONFIRMATION)
    assert not fake.calls

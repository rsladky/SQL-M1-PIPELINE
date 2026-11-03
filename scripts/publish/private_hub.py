"""Préparer, puis (sur autorisation distincte) déposer un agrégat NOAA en repo HF PRIVÉ.

Aucun dépôt n'est créé par ce module. Aucune donnée détaillée/AISStream ne peut
être sélectionnée ; pas de mode public ni de création de tag. Un mois complet,
un dossier de revue juridique locale et une confirmation CLI sont obligatoires.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
from huggingface_hub import CommitOperationAdd, HfApi

from scripts.transform.aggregate import MIN_OBSERVATIONS, MIN_VESSELS, PUBLIC_SCHEMA, candidate
from scripts.transform.batches import sha256

CARD_TEMPLATE = Path(__file__).resolve().parents[2] / "docs" / "DATASET_CARD_TEMPLATE.md"
REPO_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,80}/[A-Za-z0-9][A-Za-z0-9._-]{0,80}\Z")
MONTH = re.compile(r"\d{4}-(0[1-9]|1[0-2])\Z")
APPROVAL_FIELDS = {
    "rights_status", "private_upload_authorized", "privacy_review", "reviewer",
    "reviewed_on", "month", "region", "candidate_sha256", "repo_id",
    "source_product", "evidence_file", "evidence_sha256", "citation",
    "conditions_summary",
}
PRODUCT = "NOAA Marine Cadastre AIS Broadcast Points 2024 CSV.zst"
CONFIRMATION = "I_AUTHORIZE_PRIVATE_HF_UPLOAD"


@dataclass(frozen=True)
class Ready:
    month: str
    repo_id: str
    sha256: str
    data: bytes
    card: bytes


def _approval(path: Path, *, month: str, repo_id: str, digest: str) -> dict:
    """Check a LOCAL attestation + written evidence; human legal review remains essential."""
    if not path.is_file():
        raise PermissionError("written NOAA rights review missing; no Hub upload")
    if path.parent.name != "approvals" or path.parent.parent.name != "data":
        raise PermissionError("approval and evidence must be local under data/approvals (Git-ignored)")
    review = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(review, dict) or set(review) != APPROVAL_FIELDS:
        raise PermissionError("approval fields missing or unexpected")
    if (review["rights_status"] != "confirmed_written_noaa_permission" or
            review["private_upload_authorized"] is not True or
            review["privacy_review"] != "passed" or
            review["month"] != month or review["region"] != "ny-nj" or
            review["candidate_sha256"] != digest or review["repo_id"] != repo_id or
            review["source_product"] != PRODUCT):
        raise PermissionError("rights/privacy review not confirmed for this exact NOAA artifact/repo")
    reviewed = date.fromisoformat(review["reviewed_on"])
    age = (datetime.now(timezone.utc).date() - reviewed).days
    if age < 0 or age > 90:
        raise PermissionError("rights review must be dated within the last 90 days")
    if not isinstance(review["reviewer"], str) or len(review["reviewer"].strip()) < 3:
        raise PermissionError("identified human reviewer required")
    evidence_name = review["evidence_file"]
    if (not isinstance(evidence_name, str) or not re.fullmatch(r"[A-Za-z0-9_-]+\.(pdf|eml|txt)", evidence_name)
            or not (path.parent / evidence_name).is_file()):
        raise PermissionError("locally stored written permission evidence missing")
    evidence = path.parent / evidence_name
    if not 30 <= evidence.stat().st_size <= 2_000_000 or sha256(evidence) != review["evidence_sha256"]:
        raise PermissionError("written permission evidence checksum/size invalid")
    for key in ("citation", "conditions_summary"):
        value = review[key]
        if (not isinstance(value, str) or not 30 <= len(value) <= 600 or
                any(marker in value.lower() for marker in ("placeholder", "replace", "aisstream", "hf_token")) or
                re.search(r"\b\d{9}\b", value) or "{{" in value or "}}" in value):
            raise PermissionError(f"reviewed {key} missing or contains unsafe text")
    if not review["citation"].startswith("NOAA Office for Coastal Management. (2024)."):
        raise PermissionError("NOAA source citation not reviewed")
    return review


def _card(month: str, citation: str, conditions: str) -> bytes:
    template = CARD_TEMPLATE.read_text(encoding="utf-8")
    values = {
        "{{MONTH}}": month, "{{REGION}}": "ny-nj", "{{YEAR}}": month[:4],
        "{{DATA_FILE}}": f"data/noaa_aggregate_{month}.parquet",
        "{{CITATION}}": citation, "{{CONDITIONS}}": conditions,
    }
    for marker, value in values.items():
        template = template.replace(marker, value)
    if re.search(r"\{\{[^}]+\}\}", template):
        raise ValueError("Dataset Card template contains unresolved placeholders")
    return template.encode("utf-8")


def preflight(
    month: str, repo_id: str, approval_path: Path, *,
    raw_root: Path = Path("data/raw"), processed_root: Path = Path("data/processed"),
) -> Ready:
    """Offline only. Recomputes full-month provenance and privacy thresholds.

    Fails on missing pilot days *before* reading token or contacting HF. The
    regenerated artifact must byte-match the previously approved candidate.
    """
    if not MONTH.fullmatch(month) or not month.startswith("2024-") or not REPO_ID.fullmatch(repo_id):
        raise ValueError("2024 NOAA month and namespace/dataset repo ID required")
    base = processed_root.resolve() / "noaa_candidate"
    path = base / f"{month}-ny-nj.parquet"
    manifest = path.with_suffix(".manifest.json")
    if not path.is_file() or not manifest.is_file():
        raise ValueError("NOAA-only monthly candidate absent; no dataset to upload")
    if not 0 < path.stat().st_size <= 1_000_000:
        raise ValueError("NOAA aggregate candidate exceeds one-row safety limit")
    original_bytes = path.read_bytes()
    original_hash = hashlib.sha256(original_bytes).hexdigest()
    original_manifest = json.loads(manifest.read_text(encoding="utf-8"))
    if original_manifest.get("sha256") != original_hash:
        raise ValueError("candidate manifest SHA-256 mismatch")
    result = candidate(month, zone="ny-nj", raw_root=raw_root, processed_root=processed_root)
    if result.get("status") != "candidate_not_approved_for_publication":
        raise ValueError(f"NOAA month suppressed: {result.get('reason')}")
    if result != original_manifest or sha256(path) != original_hash:
        raise ValueError("candidate changed after human review; re-approve before upload")
    if (set(result) != {"source", "status", "month", "region", "sha256", "columns", "note"} or
            result["source"] != "noaa" or result["month"] != month or
            result["region"] != "ny-nj" or result["columns"] != PUBLIC_SCHEMA.names):
        raise ValueError("candidate provenance/column allowlist failed")
    parquet = pq.ParquetFile(path)
    if (not parquet.schema_arrow.equals(PUBLIC_SCHEMA, check_metadata=True) or
            parquet.metadata.num_rows != 1 or
            set(parquet.metadata.metadata or {}) - {b"ARROW:schema"}):
        raise ValueError("unsafe public Parquet schema or embedded metadata")
    row = parquet.read().to_pylist()[0]
    if (set(row) != set(PUBLIC_SCHEMA.names) or row["month"] != month or
            row["region"] != "ny-nj" or row["cargo_vessels"] < MIN_VESSELS or
            row["observations"] < MIN_OBSERVATIONS):
        raise ValueError("candidate row/month/privacy threshold violation")
    if b"aisstream" in original_bytes.lower():
        raise ValueError("AISStream bytes detected in candidate")
    review = _approval(approval_path, month=month, repo_id=repo_id, digest=original_hash)
    return Ready(month, repo_id, original_hash, original_bytes,
                 _card(month, review["citation"], review["conditions_summary"]))


def upload_private(
    ready: Ready, *, approval_path: Path,
    raw_root: Path = Path("data/raw"), processed_root: Path = Path("data/processed"),
    api: HfApi | None = None, token: str | None = None,
    confirmation: str | None = None,
) -> str:
    """Recheck every offline gate; never create repos or change visibility."""
    if confirmation != CONFIRMATION:
        raise PermissionError("separate explicit private upload confirmation required")
    fresh = preflight(ready.month, ready.repo_id, approval_path,
                      raw_root=raw_root, processed_root=processed_root)
    if fresh != ready:
        raise ValueError("candidate/card changed since preflight; refusing remote upload")
    token = token or os.environ.get("HF_TOKEN")
    if not token:
        raise PermissionError("HF_TOKEN missing; refusing all Hub requests")
    api = api or HfApi()
    info = api.repo_info(repo_id=ready.repo_id, repo_type="dataset", token=token)
    if info.private is not True or info.id != ready.repo_id:
        raise PermissionError("existing Hugging Face dataset must be verified PRIVATE")
    existing = set(api.list_repo_files(repo_id=ready.repo_id, repo_type="dataset", token=token))
    if existing - {".gitattributes"}:
        raise PermissionError("dataset repo is not empty; manual review required")
    operations = [
        CommitOperationAdd(path_in_repo="README.md", path_or_fileobj=ready.card),
        CommitOperationAdd(path_in_repo=f"data/noaa_aggregate_{ready.month}.parquet",
                           path_or_fileobj=ready.data),
    ]
    api.create_commit(repo_id=ready.repo_id, repo_type="dataset", token=token,
                      commit_message=f"Add private reviewed NOAA aggregate {ready.month}",
                      operations=operations)
    verified = api.repo_info(repo_id=ready.repo_id, repo_type="dataset", token=token)
    if verified.private is not True:
        raise PermissionError("STOP: remote dataset visibility changed; inspect immediately")
    return f"https://huggingface.co/datasets/{ready.repo_id}"


def main() -> None:
    parser = argparse.ArgumentParser(description="NOAA private Hub gate; offline by default")
    parser.add_argument("month", help="YYYY-MM calendar month, all daily NOAA units required")
    parser.add_argument("repo_id", help="existing PRIVATE namespace/dataset repo ID")
    parser.add_argument("--approval", type=Path, default=Path("data/approvals/noaa-hub.json"))
    parser.add_argument("--upload", action="store_true", help="requires separate approval and explicit confirmation")
    parser.add_argument("--confirm-private-upload", help=f"literal {CONFIRMATION} required")
    args = parser.parse_args()
    ready = preflight(args.month, args.repo_id, args.approval)
    if args.upload:
        print(upload_private(ready, approval_path=args.approval,
                             confirmation=args.confirm_private_upload))
    else:
        print(f"Offline preflight OK for {ready.month}; no network calls, upload or repo creation")


if __name__ == "__main__":
    main()

"""Préparer, puis (sur autorisation distincte) déposer un agrégat NOAA en repo HF PRIVÉ.

Aucun dépôt n'est créé par ce module. Aucune donnée détaillée/AISStream ne peut
être sélectionnée ; pas de mode public ni de création de tag. Un mois complet,
une attestation locale de revue des sources publiées et une confirmation CLI sont obligatoires.
Cette attestation n'est PAS une permission individuelle NOAA ni un avis juridique.
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
from urllib.parse import unquote, urlsplit

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
    "repo_owner_attestation", "source_product", "source_product_url",
    "faq_url", "faq_revision", "faq_reviewed_on", "product_notice_url",
    "product_notice_revision", "product_notice_reviewed_on",
    "product_notice_matches_csv2024",
    "evidence_file", "evidence_sha256", "citation", "conditions_summary",
    "derived_product_analysis", "uscg_restriction_analysis",
    "purpose_compatibility_analysis", "privacy_risk_analysis", "residual_uncertainty",
}
PRODUCT = "NOAA Marine Cadastre AIS Broadcast Points 2024 CSV.zst"
PRODUCT_INDEX = "https://noaaocm.blob.core.windows.net/ais/csv2/csv2024/index.html"
AIS_CATALOG_INDEX = "https://www.fisheries.noaa.gov/inport/item/55360"  # not the 2024 product notice
FAQ_URL = "https://coast.noaa.gov/data/marinecadastre/ais/faq.pdf"
REVIEW_STATUS = "documented_public_sources_private_only"
OWNER_ATTESTATION = "I_CONTROL_THIS_PRIVATE_REPO_AND_ACCEPT_REVIEW_RISK"
CONFIRMATION = "I_AUTHORIZE_PRIVATE_HF_UPLOAD"


@dataclass(frozen=True)
class Ready:
    month: str
    repo_id: str
    sha256: str
    data: bytes
    card: bytes
    approval_sha256: str
    evidence_sha256: str


def _text(review: dict, key: str, *, min_length: int = 30, max_length: int = 750) -> str:
    """Reject empty/multiline text, identifier-like strings and known secret/claim markers."""
    value = review[key]
    if (not isinstance(value, str) or not min_length <= len(value.strip()) <= max_length or
            value != value.strip() or any(ord(c) < 32 for c in value) or
            re.search(r"\b\d{9}\b", value) or "{{" in value or "}}" in value or
            any(s in value.lower() for s in (
                "placeholder", "hf_token", "synthetic", "permission noaa confirmed",
                "noaa permission confirmed", "noaa approval granted", "license: cc0",
                "licence: cc0", "cc0 licensed",
            ))):
        raise PermissionError(f"reviewed {key} missing or unsafe; no Hub upload")
    return value


def _official_notice(url: str) -> bool:
    if not isinstance(url, str):
        return False
    try:
        parsed = urlsplit(url)
        host = parsed.hostname or ""
        if (host in ("fisheries.noaa.gov", "www.fisheries.noaa.gov") and
                unquote(parsed.path).rstrip("/").lower() == urlsplit(AIS_CATALOG_INDEX).path):
            return False  # index AIS, never proof of the 2024 product notice
        return (parsed.scheme == "https" and parsed.username is None and
                parsed.password is None and parsed.port is None and
                not parsed.query and not parsed.fragment and len(parsed.path) > 8 and
                (host.endswith(".noaa.gov") or host in ("marinecadastre.gov", "hub.marinecadastre.gov")))
    except ValueError:  # malformed port, URL or bracketed address
        return False


def _approval(path: Path, *, month: str, repo_id: str, digest: str) -> tuple[dict, str]:
    """Validate local human interpretation, NOT NOAA approval or legal truth."""
    if not path.is_file() or path.is_symlink() or path.parent.is_symlink():
        raise PermissionError("local documented NOAA/USCG review missing; no Hub upload")
    if (path.parent.name != "approvals" or path.parent.parent.name != "data" or
            path.resolve().parent.name != "approvals" or
            path.resolve().parent.parent.name != "data"):
        raise PermissionError("review and evidence must stay under data/approvals (Git-ignored)")
    raw = path.read_bytes()
    try:
        review = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise PermissionError("invalid local review JSON") from None
    if not isinstance(review, dict) or set(review) != APPROVAL_FIELDS:
        raise PermissionError("review fields missing or unexpected")
    if (review["rights_status"] != REVIEW_STATUS or
            review["private_upload_authorized"] is not True or
            review["privacy_review"] != "passed_with_residual_risk" or
            review["repo_owner_attestation"] != OWNER_ATTESTATION or
            review["month"] != month or review["region"] != "ny-nj" or
            review["candidate_sha256"] != digest or review["repo_id"] != repo_id or
            review["source_product"] != PRODUCT or
            review["source_product_url"] != PRODUCT_INDEX or
            review["faq_url"] != FAQ_URL or
            review["product_notice_matches_csv2024"] is not True or
            not _official_notice(review["product_notice_url"])):
        raise PermissionError("documented terms/privacy/ownership review not confirmed for this artifact/repo")
    today = datetime.now(timezone.utc).date()
    dates = []
    for key in ("reviewed_on", "faq_reviewed_on", "product_notice_reviewed_on"):
        try:
            checked = date.fromisoformat(review[key])
        except (ValueError, TypeError):
            raise PermissionError(f"invalid {key}; review must be dated") from None
        if not 0 <= (today - checked).days <= 90:
            raise PermissionError("source/review dates must be within the last 90 days")
        dates.append(checked)
    if dates[1] > dates[0] or dates[2] > dates[0]:
        raise PermissionError("source documents must be reviewed before attestation")
    _text(review, "reviewer", min_length=3, max_length=120)
    for key in ("faq_revision", "product_notice_revision"):
        _text(review, key, min_length=8, max_length=150)
    for key in (
        "citation", "conditions_summary", "derived_product_analysis",
        "uscg_restriction_analysis", "purpose_compatibility_analysis",
        "privacy_risk_analysis", "residual_uncertainty",
    ):
        _text(review, key)
    if (not review["citation"].startswith("NOAA Office for Coastal Management.") or
            any(part not in review["citation"] for part in (
                "2024", "AIS", "CSV.zst", "Marine Cadastre", PRODUCT_INDEX,
            ))):
        raise PermissionError("2024 NOAA daily CSV.zst product citation not documented")
    evidence_name = review["evidence_file"]
    if (not isinstance(evidence_name, str) or
            not re.fullmatch(r"[A-Za-z0-9_-]+\.md", evidence_name)):
        raise PermissionError("local source-review notes (.md) required")
    evidence = path.parent / evidence_name
    if not evidence.is_file() or evidence.is_symlink():
        raise PermissionError("local source-review notes missing or linked")
    content = evidence.read_bytes()
    if (not 350 <= len(content) <= 100_000 or
            hashlib.sha256(content).hexdigest() != review["evidence_sha256"] or
            any(marker.encode() not in content for marker in (
                FAQ_URL, review["product_notice_url"], PRODUCT_INDEX, digest, repo_id,
            ))):
        raise PermissionError("source-review notes are missing, stale or lack product/repo links")
    return review, hashlib.sha256(raw).hexdigest()


def _card(month: str, review: dict) -> bytes:
    template = CARD_TEMPLATE.read_text(encoding="utf-8")
    values = {
        "{{MONTH}}": month, "{{REGION}}": "ny-nj", "{{YEAR}}": month[:4],
        "{{DATA_FILE}}": f"data/noaa_aggregate_{month}.parquet",
        "{{CITATION}}": review["citation"], "{{CONDITIONS}}": review["conditions_summary"],
        "{{FAQ_URL}}": review["faq_url"], "{{FAQ_REVISION}}": review["faq_revision"],
        "{{FAQ_DATE}}": review["faq_reviewed_on"],
        "{{NOTICE_URL}}": review["product_notice_url"],
        "{{NOTICE_REVISION}}": review["product_notice_revision"],
        "{{NOTICE_DATE}}": review["product_notice_reviewed_on"],
        "{{DERIVED_ANALYSIS}}": review["derived_product_analysis"],
        "{{USCG_ANALYSIS}}": review["uscg_restriction_analysis"],
        "{{PURPOSE_ANALYSIS}}": review["purpose_compatibility_analysis"],
        "{{PRIVACY_ANALYSIS}}": review["privacy_risk_analysis"],
        "{{UNCERTAINTY}}": review["residual_uncertainty"],
    }
    for marker, value in values.items():
        if marker not in template:
            raise ValueError(f"Dataset Card template marker missing: {marker}")
        template = template.replace(marker, value)
    if re.search(r"\{\{[^}]+\}\}", template):
        raise ValueError("Dataset Card template contains unresolved placeholders")
    return template.encode("utf-8")


def preflight(
    month: str, repo_id: str, approval_path: Path, *,
    raw_root: Path = Path("data/raw"), processed_root: Path = Path("data/processed"),
) -> Ready:
    """Offline only. Recomputes full-month provenance and privacy thresholds.

    Fails on missing days *before* reading token or contacting HF. The
    regenerated artifact must byte-match the previously reviewed candidate.
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
    review, review_hash = _approval(approval_path, month=month, repo_id=repo_id, digest=original_hash)
    return Ready(month, repo_id, original_hash, original_bytes,
                 _card(month, review), review_hash, review["evidence_sha256"])


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
    parser.add_argument("--upload", action="store_true", help="requires reviewed local attestation and separate upload confirmation")
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

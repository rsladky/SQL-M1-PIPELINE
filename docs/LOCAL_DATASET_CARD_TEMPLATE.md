---
pretty_name: NOAA Marine Cadastre historical AIS cargo aggregate — local draft
language:
  - en
configs:
  - config_name: default
    data_files:
      - split: train
        path: {{DATA_FILE}}
dataset_info:
  features:
    - name: month
      dtype: string
    - name: region
      dtype: string
    - name: cargo_vessels
      dtype: int64
    - name: observations
      dtype: int64
---

# NOAA Marine Cadastre cargo aggregate — {{MONTH}}, NY/NJ

**STATUS: DRAFT LOCAL ONLY — NOT PUBLISHED, NOT AUTHORIZED FOR UPLOAD.** This README and its single aggregate Parquet reside solely in a Git-ignored local folder. No Hugging Face repository or upload has been created. A complete month and statistical thresholds do not establish legal rights or eliminate disclosure risks.

## Scope, fields and intended uses

One historical UTC calendar month ({{MONTH}}) and one broad NY/NJ coastal rectangle, approximately **39°–42° N, 75°–71° W**. Source: U.S. coastal and inland land-receiver AIS broadcast-point archives; observations are **not** complete worldwide voyages or live locations. This file contains exactly one aggregate row, under `{{DATA_FILE}}`. The Hub-style `train` split above is metadata for a *possible future* loader, not evidence that a dataset was uploaded. Intended descriptive use, if separately authorized, is coastal/ocean planning; **not navigation, enforcement, surveillance or vessel-level decisions**.

| Field | Type | Meaning |
| --- | --- | --- |
| `month` | `string` | Complete historical UTC month `{{MONTH}}`; not an exact observation date. |
| `region` | `string` | Broad fixed `ny-nj` analysis rectangle; not an exact position or port. |
| `cargo_vessels` | `int64` | Distinct source-scoped AIS MMSI transmitters classified as cargo (70–79), **not proven physical vessels or specifically container ships**. |
| `observations` | `int64` | Valid NOAA AIS broadcast points; neither journeys, vessel owners nor cargo volumes. |

## Origin and preparation

**Product:** NOAA Office for Coastal Management, Marine Cadastre, historical {{YEAR}} AIS Broadcast Points, **daily national CSV.zst** archives, accessed via [official index](https://noaaocm.blob.core.windows.net/ais/csv2/csv{{YEAR}}/index.html). All {{DAYS}} daily archive URLs, local JSONL checksums and transformed NOAA-only Parquet manifests were checked for the month. The monthly candidate was filtered to the broad rectangle and validated again *read-only* before the local package was copied. AIS code `vessel_type` 70–79 denotes the reported/corrected cargo group; it does not identify container ships. Invalid/duplicate positions and malformed identifiers were discarded; impossible position jumps/conflicts were excluded from the aggregate. Receiver gaps remain uncertain. The final local Parquet has **no MMSI/IMO, names, exact coordinates, timestamps, daily breakdown or trajectories**, and it contains **no AISStream observations**.

Minimum release-screening rules implemented locally: full calendar month present, ≥20 distinct MMSI transmitters, ≥200 valid points, ≥7 days with observations, and no one MMSI contributing >10% of valid points. They are **necessary safeguards, not an anonymity guarantee or release authorization**. Correlation against other AIS sources, exceptional vessels and differencing across later revisions/overlapping releases require human review; do not make multiple versions or finer slices without reassessing the risk.

## Citation, legal status and limitations

**Provisional source attribution (exact product title to recheck against NOAA metadata before any distribution):** NOAA Office for Coastal Management. ({{YEAR}}). AIS Broadcast Points. Marine Cadastre. https://marinecadastre.gov.

References: [NOAA AIS data page](https://marinecadastre.gov/ais/), [2024 CSV.zst index](https://noaaocm.blob.core.windows.net/ais/csv2/csv2024/index.html), [data dictionary](https://coast.noaa.gov/data/marinecadastre/ais/data-dictionary.pdf), [candidate 2024 product notice/metadata (exact link to the CSV.zst `csv2` archive not yet confirmed)](https://www.fisheries.noaa.gov/inport/item/73064), [AIS FAQ — Appropriate Use](https://coast.noaa.gov/data/marinecadastre/ais/faq.pdf). NOAA's FAQ permits release of derived products with citation **while also quoting U.S. Coast Guard restrictions on retransmission/redistribution of AIS information outside approved purposes**. The precise product notice, rights to host even a derived private Hub dataset, and any license for this product have **not been established**. **No CC0, open-data or other license is asserted or granted here.** This local draft does not imply NOAA approval. The separate Hub upload guard requires a **local human-documented review** of the exact NOAA product notice, the FAQ/USCG restrictions and disclosure risks, plus later explicit private-upload authorization and a local token; it does not establish NOAA-specific permission or legal certainty. This local draft is **not** that review or an upload authorization.

Marine Cadastre coverage is limited by land-receiver availability, missing broadcasts, one-minute filtering and potential sensor outages. Type codes and positions can be inaccurate or corrected; MMSI assignment can change. Counts cannot estimate all global shipping, complete routes, unique owners, cargo tonnage, current vessel positions or causal trends. Historical one-month/one-region data are insufficient to infer trajectories reliably, but residual disclosure risk still needs assessment.

**Stewardship:** project team; contacts, authorization, license/conditions statement, Hub URL and release version are deliberately absent pending explicit review. This document is a **local draft Dataset Card**, not a public/private Hub listing or legal permission. No `v1.0.0` release or public visibility is authorized.

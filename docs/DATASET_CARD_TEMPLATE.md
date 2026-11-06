---
pretty_name: NOAA Marine Cadastre Cargo Broadcast-Point Aggregates (NY/NJ)
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

**Status: private derived-product review only. Not approved for public release.** This card is rendered after a human-documented interpretation of published source terms and disclosure risks, **not** an individual NOAA permission or legal opinion. The repository must already exist and be private; rendering this card uploads nothing. No AISStream data, positions, identifiers or vessel-level sequences are included in the aggregate Parquet.

## Dataset summary and purpose

One record per **complete UTC calendar month** and one fixed, broad coastal region. The underlying NOAA AIS observations are historical land-receiver broadcast points, not a complete worldwide track or count of all voyages. Intended only as a coarse descriptive aid to coastal/ocean planning; **not for navigation, surveillance, regulatory or enforcement decisions**. The historical one-day pilot alone did not produce a monthly record; the January 2024 candidate used here requires every daily NOAA unit and a complete UTC month.

## Fields and unit of analysis

| Field | Type | Meaning |
| --- | --- | --- |
| `month` | string `YYYY-MM` | Entire calendar month, UTC; no daily or precise observation times. |
| `region` | string | Fixed `{{REGION}}` NY/NJ coastal rectangle, approximately 39°–42° N, 75°–71° W; not a ship location. |
| `cargo_vessels` | int64 | Distinct source-scoped **MMSI transmitters** observed with NOAA `vessel_type` 70–79; not proven unique physical vessels or container ships. |
| `observations` | int64 | Number of valid NOAA broadcast points (not ships, voyages, cargo loads or distances). |

Period covered by **this file**: {{MONTH}} (UTC). Source year: {{YEAR}}. Geographic coverage: only NOAA-receiver observations in the NY/NJ rectangle above; coverage may be sparse/interrupted and points outside the rectangle are not represented. The Parquet file path is `{{DATA_FILE}}`; exactly one row is expected. No training/validation split: the `train` designation above is merely the Hub's data-file loading convention.

## Construction, quality and safeguards

- Source: [NOAA Marine Cadastre AIS 2024 daily, national CSV.zst broadcast points](https://noaaocm.blob.core.windows.net/ais/csv2/csv2024/index.html), locally filtered to the fixed broad region; **no AISStream observations**. All daily source and transform manifests for the full UTC month are validated locally against checksums and official NOAA archive URL before building the candidate.
- `vessel_type` 70–79 is the reported/corrected AIS cargo group, not a container-ship classifier. Invalid positions, malformed identifiers/timestamps and exact duplicate points are removed. Missing AIS SOG/COG/heading sentinels become null. Suspected impossible position jumps/conflicts are excluded from aggregate counts; reception gaps remain explicitly uncertain.
- An aggregate is suppressed unless it has ≥20 distinct MMSI, ≥200 valid observations, ≥7 distinct days with observations **and** input files for every day of the calendar month. It is also suppressed if one MMSI contributes >10% of counted observations. No fine grid, port, daily series, individual trace, ship name, MMSI/IMO or exact coordinates/times are published.
- Thresholds do **not** prove anonymity. A human review must check contribution skews, linked external datasets, time/region uniqueness and **differencing across releases**. Do not add overlapping time/region tables or update an already exposed dataset without a renewed disclosure-risk assessment.

## Provenance, citation and usage conditions

**Provisional product attribution recorded by the human reviewer (verify exact metadata title):** {{CITATION}}

Source: [NOAA Marine Cadastre 2024 national daily AIS CSV.zst index](https://noaaocm.blob.core.windows.net/ais/csv2/csv2024/index.html), not an AISStream stream or an assumed CC0 CSV release. Human-reviewed published texts (URLs/revision labels/access dates recorded, but their contents are **not** independently certified by this script):

- [NOAA AIS FAQ — Appropriate Use]({{FAQ_URL}}), revision/edition: {{FAQ_REVISION}}; reviewed {{FAQ_DATE}}.
- [Exact NOAA CSV.zst source-product notice/metadata]({{NOTICE_URL}}), revision/edition: {{NOTICE_REVISION}}; reviewed {{NOTICE_DATE}}. The reviewer must verify this notice applies to **this 2024 CSV.zst archive**, not another AIS product.
- Reference: [AIS data dictionary](https://coast.noaa.gov/data/marinecadastre/ais/data-dictionary.pdf).

The FAQ discusses releasing derived products with citation while quoting U.S. Coast Guard limits on retransmission/redistribution of AIS information outside approved purposes. **That tension is not settled by this card, the script or private Hub visibility.** The following are the reviewer's **documented interpretation**, not NOAA endorsement:

- Derived-product clause and attribution: {{DERIVED_ANALYSIS}}
- USCG AIS-sharing restriction applied to this aggregate: {{USCG_ANALYSIS}}
- Intended-use compatibility and restrictions: {{PURPOSE_ANALYSIS}}
- Terms/conditions for this private-only scope: {{CONDITIONS}}
- Human privacy, linkage and differencing assessment: {{PRIVACY_ANALYSIS}}
- Residual legal and reidentification uncertainty: {{UNCERTAINTY}}

**License:** No NOAA individual permission is claimed; **no CSV.zst CC0 or other open license is asserted**, and this project grants no unrestricted permission for reuse/retransmission of AIS information. The review reflects a risk-bearing interpretation of public materials, **not legal advice**. If the product notice or USCG scope is unclear, pause and seek clarification from NOAA (`MarineCadastre@noaa.gov`); an empty or ambiguously authorized private dataset is safer than uploading contested data. **Do not make this dataset public, tag a release or redistribute it without separate documented authorization and privacy review.**

## Limitations and appropriate use

Marine Cadastre data originate primarily from U.S. land-based receivers, are filtered to one-minute resolution, omit some areas/vessels and experience sensor outages. Historical MMSI assignments can change; AIS positions and vessel type can be erroneous or imputed by NOAA AVID. Counts cannot represent complete global voyages, cargo quantities, ownership, real-time traffic, ship identity or safe navigation. The single broad-zone/month design limits spatial/temporal inference but may suppress many months. Record selection, quality rules and legal constraints affect comparisons between periods. No AISStream coverage claim is made from this product.

## Stewardship and versioning

Maintainer: project team (contact to be added only with consent). This model card is **not a license or public release**. The private repository initially contains only this README and one NOAA-only aggregate; no raw JSONL/CSV, detailed Parquet, identifiers, approval documents, secrets or AISStream files. A public URL/version and Hub `v1.0.0` tag must be added **only after** separate authorization and final review, not by the preparation script.

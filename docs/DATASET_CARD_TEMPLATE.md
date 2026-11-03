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

**Status: private review dataset only. Not approved for public release.** This card is a template rendered only after a documented legal and privacy review; the repository must already exist and be private. No data have been uploaded merely by creating this template. No AISStream data, positions, identifiers or vessel-level sequences are included in the aggregate Parquet.

## Dataset summary and purpose

One record per **complete UTC calendar month** and one fixed, broad coastal region. The underlying NOAA AIS observations are historical land-receiver broadcast points, not a complete worldwide track or count of all voyages. Intended only as a coarse descriptive aid to coastal/ocean planning; **not for navigation, surveillance, regulatory or enforcement decisions**. The pilot 2024-01-14 by itself cannot produce a record because the rest of January is missing.

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

{{CITATION}}

Original data and metadata: [Marine Cadastre AIS](https://marinecadastre.gov/ais/), [InPort metadata](https://www.fisheries.noaa.gov/inport/item/55360), [NOAA AIS FAQ — Appropriate Use](https://coast.noaa.gov/data/marinecadastre/ais/faq.pdf), and [AIS data dictionary](https://coast.noaa.gov/data/marinecadastre/ais/data-dictionary.pdf). NOAA states that derived products may be released with citation, **but** its FAQ also quotes U.S. Coast Guard restrictions on retransmitting/redistributing AIS information outside approved purposes. A label `other`/`unknown` is not permission. The exact source-product notice and conditions must be reviewed for the planned upload and for any later visibility change.

**Reviewed permissions and conditions for this private derived-product upload:** {{CONDITIONS}}

**License:** This project assigns **no license** to the NOAA input or to this derived dataset; neither public-domain generalities nor the private upload confer an unrestricted license to reuse/retransmit AIS data. If a formal license/permission designation is established, document its exact legal text and scope after review; do not invent one. **Do not make this dataset public, tag a release or redistribute it without a separate documented authorization and privacy review.** If authorization is unclear, ask NOAA (`MarineCadastre@noaa.gov`) and leave the dataset private without contested data.

## Limitations and appropriate use

Marine Cadastre data originate primarily from U.S. land-based receivers, are filtered to one-minute resolution, omit some areas/vessels and experience sensor outages. Historical MMSI assignments can change; AIS positions and vessel type can be erroneous or imputed by NOAA AVID. Counts cannot represent complete global voyages, cargo quantities, ownership, real-time traffic, ship identity or safe navigation. The single broad-zone/month design limits spatial/temporal inference but may suppress many months. Record selection, quality rules and legal constraints affect comparisons between periods. No AISStream coverage claim is made from this product.

## Stewardship and versioning

Maintainer: project team (contact to be added only with consent). This model card is **not a license or public release**. The private repository initially contains only this README and one NOAA-only aggregate; no raw JSONL/CSV, detailed Parquet, identifiers, approval documents, secrets or AISStream files. A public URL/version and Hub `v1.0.0` tag must be added **only after** separate authorization and final review, not by the preparation script.

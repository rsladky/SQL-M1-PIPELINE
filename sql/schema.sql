-- Tables LOCALES uniquement : pas de vue publique ni de droits PUBLIC sur les données AIS.
-- MMSI = identifiant d'émetteur au sein d'une source, PAS identité permanente d'un navire.
CREATE TABLE IF NOT EXISTS sources (
    code TEXT PRIMARY KEY CHECK (code IN ('noaa', 'aisstream')),
    description TEXT NOT NULL,
    base_url TEXT NOT NULL
);
INSERT INTO sources(code, description, base_url) VALUES
    ('noaa', 'NOAA Marine Cadastre historical US receiver broadcast points', 'https://marinecadastre.gov/ais/'),
    ('aisstream', 'AISStream live stream, local use only', 'https://aisstream.io/documentation')
ON CONFLICT (code) DO NOTHING;

CREATE TABLE IF NOT EXISTS vessels (
    source_code TEXT NOT NULL REFERENCES sources(code),
    mmsi CHAR(9) NOT NULL CHECK (mmsi ~ '^[0-9]{9}$'),
    imo TEXT,
    vessel_name TEXT,
    PRIMARY KEY (source_code, mmsi)
);

CREATE TABLE IF NOT EXISTS ingestion_batches (
    source_code TEXT NOT NULL REFERENCES sources(code),
    input_name TEXT NOT NULL,
    input_sha256 CHAR(64) NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    parquet_path TEXT NOT NULL,
    parquet_sha256 CHAR(64) NOT NULL CHECK (parquet_sha256 ~ '^[0-9a-f]{64}$'),
    source_url TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_code, input_name, parquet_path),
    CHECK ((source_code = 'noaa' AND source_url IS NOT NULL AND source_url LIKE 'https://noaaocm.blob.core.windows.net/ais/csv2/%')
        OR (source_code = 'aisstream' AND source_url IS NULL))
);

CREATE TABLE IF NOT EXISTS observations (
    observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_code TEXT NOT NULL REFERENCES sources(code),
    mmsi CHAR(9) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    vessel_type SMALLINT NOT NULL CHECK (vessel_type BETWEEN 70 AND 79),
    sog REAL CHECK (sog BETWEEN 0 AND 60),
    cog REAL CHECK (cog BETWEEN 0 AND 359.9),
    heading SMALLINT CHECK (heading BETWEEN 0 AND 359),
    quality_flag TEXT NOT NULL CHECK (quality_flag IN ('ok', 'tracking_gap', 'implausible_jump', 'conflicting_position')),
    source_url TEXT,
    FOREIGN KEY (source_code, mmsi) REFERENCES vessels(source_code, mmsi),
    CHECK ((source_code = 'noaa' AND source_url IS NOT NULL AND source_url LIKE 'https://noaaocm.blob.core.windows.net/ais/csv2/%')
        OR (source_code = 'aisstream' AND source_url IS NULL)),
    UNIQUE (source_code, mmsi, observed_at, latitude, longitude)
);

-- Agrégat candidat NOAA uniquement, privé dans la même base locale.
-- La validité du mois entier et des contributions est contrôlée en amont.
CREATE TABLE IF NOT EXISTS noaa_aggregates (
    month_start DATE NOT NULL CHECK (EXTRACT(DAY FROM month_start) = 1),
    region TEXT NOT NULL CHECK (region = 'ny-nj'),
    source_code TEXT NOT NULL DEFAULT 'noaa' REFERENCES sources(code) CHECK (source_code = 'noaa'),
    cargo_vessels INTEGER NOT NULL CHECK (cargo_vessels >= 20),
    observations BIGINT NOT NULL CHECK (observations >= 200),
    candidate_sha256 CHAR(64) NOT NULL CHECK (candidate_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (month_start, region)
);

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC;

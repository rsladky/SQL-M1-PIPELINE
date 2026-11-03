-- Analyses LOCALES uniquement ; exécuter dans psql (les vues publiques sont interdites).
-- Le pilote historique NOAA du 2024-01-14 est limité au rectangle NY/NJ.
-- Les absences de signal ne démontrent pas des voyages continus ni une couverture mondiale.

-- 01. Jointure sources + GROUP BY sur nombre d'observations/cargos par jour NOAA.
-- Filtre représentatif pour EXPLAIN (source, plage de temps). Index optionnel
-- (source_code, observed_at) dans sql/indexes.sql ; comparer les plans mesurés.
SELECT s.code, s.description, date_trunc('day', o.observed_at AT TIME ZONE 'UTC')::date AS utc_day,
       COUNT(*) AS observations, COUNT(DISTINCT o.mmsi) AS distinct_mmsi
FROM observations o JOIN sources s ON s.code = o.source_code
WHERE o.source_code = 'noaa'
  AND o.observed_at >= TIMESTAMPTZ '2024-01-14 00:00:00+00'
  AND o.observed_at <  TIMESTAMPTZ '2024-01-15 00:00:00+00'
GROUP BY s.code, s.description, utc_day
ORDER BY utc_day;

-- 02. Fenêtre LAG : durée entre observations consécutives d'un MMSI.
-- Dépassement > 6 h = rupture de suivi, PAS durée de voyage. Un même MMSI
-- peut changer d'émetteur/propriétaire ; ne pas reconstruire sa trajectoire.
WITH ordered AS (
    SELECT o.source_code, o.mmsi, o.observed_at, o.quality_flag,
           LAG(o.observed_at) OVER (
               PARTITION BY o.source_code, o.mmsi ORDER BY o.observed_at, o.observation_id
           ) AS previous_time
    FROM observations o
    WHERE o.source_code = 'noaa'
)
SELECT source_code, observed_at::date AS utc_day,
       COUNT(*) FILTER (WHERE observed_at - previous_time > INTERVAL '6 hours') AS gaps_over_6h,
       ROUND(AVG(EXTRACT(EPOCH FROM (observed_at - previous_time)))::numeric, 1)
           AS mean_seconds_between_received_points
FROM ordered
GROUP BY source_code, utc_day
ORDER BY utc_day;

-- 03. CTE + fenêtre DENSE_RANK : émetteurs les plus observés par source.
-- Les rangs sont locaux à chaque source ; MMSI ne signifie pas navire unique stable.
WITH counts AS (
    SELECT source_code, mmsi, COUNT(*) AS point_count
    FROM observations GROUP BY source_code, mmsi
), ranked AS (
    SELECT source_code, mmsi, point_count,
           DENSE_RANK() OVER (PARTITION BY source_code ORDER BY point_count DESC) AS rank_in_source
    FROM counts
)
SELECT r.source_code, v.mmsi, r.point_count, r.rank_in_source
FROM ranked r JOIN vessels v ON (v.source_code, v.mmsi) = (r.source_code, r.mmsi)
WHERE r.rank_in_source <= 5
ORDER BY r.source_code, r.rank_in_source, v.mmsi;

-- 04. GROUP BY qualité / types déclarés : sauts, lacunes, types 70–79.
-- Les points aberrants ne prouvent aucun déplacement physique du bateau.
SELECT s.code AS source, o.vessel_type, o.quality_flag,
       COUNT(*) AS observations, COUNT(DISTINCT o.mmsi) AS distinct_mmsi
FROM observations o JOIN sources s ON s.code = o.source_code
GROUP BY s.code, o.vessel_type, o.quality_flag
ORDER BY s.code, o.vessel_type, o.quality_flag;

-- 05. Comparaison d'un agrégat privé candidat NOAA avec les points locaux du
-- même mois et de la seule zone NY/NJ ; absence de ligne = aucune publication.
-- Ne PAS interpréter l'écart en série de traversées complètes. N'exporter ni
-- données détaillées ni cette requête avec identifiants/horaires individuels.
WITH local_points AS (
    SELECT date_trunc('month', observed_at AT TIME ZONE 'UTC')::date AS month_start,
           COUNT(*) FILTER (WHERE quality_flag IN ('ok','tracking_gap')) AS good_points,
           COUNT(DISTINCT mmsi) FILTER (WHERE quality_flag IN ('ok','tracking_gap')) AS local_mmsi
    FROM observations WHERE source_code = 'noaa'
    GROUP BY date_trunc('month', observed_at AT TIME ZONE 'UTC')::date
)
SELECT a.month_start, a.region, a.cargo_vessels, a.observations,
       l.local_mmsi, l.good_points
FROM noaa_aggregates a LEFT JOIN local_points l ON l.month_start = a.month_start
WHERE a.source_code = 'noaa'
ORDER BY a.month_start, a.region;

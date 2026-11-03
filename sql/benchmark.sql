-- Fenêtre d'une heure de l'analyse 01 (mêmes jointure et GROUP BY).
-- À exécuter avec EXPLAIN (ANALYZE, BUFFERS) avant et après sql/indexes.sql.
-- Mesures dépendantes du cache, du volume et de la machine : ne pas généraliser.
SELECT s.code, date_trunc('day', o.observed_at AT TIME ZONE 'UTC')::date AS utc_day,
       COUNT(*) AS observations, COUNT(DISTINCT o.mmsi) AS distinct_mmsi
FROM observations o JOIN sources s ON s.code = o.source_code
WHERE o.source_code = 'noaa'
  AND o.observed_at >= TIMESTAMPTZ '2024-01-14 12:00:00+00'
  AND o.observed_at < TIMESTAMPTZ '2024-01-14 13:00:00+00'
GROUP BY s.code, utc_day
ORDER BY utc_day;

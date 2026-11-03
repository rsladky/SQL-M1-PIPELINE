-- À appliquer APRÈS mesure du plan initial de sql/analyses.sql (requête 01).
-- Accélère potentiellement le filtrage source + plage temporelle ; sur un petit
-- lot la lecture séquentielle peut rester plus rapide. Ne jamais promettre un gain.
CREATE INDEX IF NOT EXISTS observations_source_time_idx
    ON observations (source_code, observed_at);

# PostgreSQL local — étape 4 (mesures du pilote, 2026-11-03)

**Usage strictement local.** La base et son volume Docker contiennent des positions, identifiants et noms de navires ; ne jamais les exporter vers GitHub ou Hugging Face, ni exposer le port publiquement. Le chargeur refuse explicitement une `DATABASE_URL` pointant hors de la boucle locale (`127.0.0.1`, `localhost` ou `::1`). Aucun flux AISStream réel n'a été capturé faute de clé. Le pilote NOAA est une seule journée dans NY/NJ et n'est pas un trajet mondial.

## Démarrage, secret, chargement

1. Installer Docker Compose et Python 3.11–3.14 ; installer les dépendances du `pyproject.toml` (`psycopg[binary]`, `pyarrow`, `pytest` pour les tests).
2. Copier `.env.example` vers `.env` **non suivi par Git** ; définir `POSTGRES_PASSWORD` fort, puis `DATABASE_URL=postgresql://ais_user:<mot-de-passe-encodé-URL>@127.0.0.1:5432/ais_pipeline` avec **le même** mot de passe. Garder `.env` en mode 0600. Les valeurs de `.env.example` sont des placeholders, jamais des secrets. Option `POSTGRES_PORT` si le port local 5432 est occupé (modifier aussi DATABASE_URL). Pour la commande de l'étape 4, ce fichier doit contenir des **affectations valides pour le shell** : protéger/quotter les valeurs ayant des caractères spéciaux pour le shell. **Ne sourcer que son propre `.env` de confiance** : sourcer un fichier exécute son contenu comme du code.
3. `docker compose up -d --wait postgres` : image PostgreSQL 16, port lié à **127.0.0.1 uniquement** ; `sql/schema.sql` initialise les tables au premier démarrage **sur volume vide**. Sur un volume préexistant, appliquer les changements de schéma explicitement via psql/migration avant chargement ; ne pas supprimer un volume contenant des données par erreur.
4. **Docker Compose lit `.env`, mais Python ne le lit pas automatiquement.** Depuis la racine du dépôt, charger `DATABASE_URL` dans un sous-shell, sans coller le secret dans la commande ni le laisser exporté après exécution :

   ```sh
   (
     . ./.env && export DATABASE_URL && \
       .venv/bin/python -m scripts.load.postgres \
         data/processed/source/noaa/manifests/2024-01-14-ny-nj.json
   )
   ```

   Seule `DATABASE_URL` est exportée au processus Python ; les autres variables de `.env` restent locales au sous-shell, sous réserve qu'elles ne soient pas déjà exportées. Charger un manifeste produit par la transformation. Le programme valide SHA-256/provenance de l'entrée brute et du Parquet, le schéma, la partition, le `source` et le type cargo ; il ne charge pas le JSONL directement. Il écrit par lots de 5 000 au maximum dans **une transaction par manifeste**, avec clés étrangères et `ON CONFLICT`. Deuxième exécution inchangée = **0 insertion**. Un test d'intégration injecte une deuxième ligne invalide après un premier lot valide et confirme le rollback total (0 observation, 0 navire, 0 lot). Si un lot déjà chargé change, erreur bloquante : reconstruire la base de façon contrôlée au lieu de laisser d'anciennes observations orphelines.
5. Pour une capture AISStream locale terminée, utiliser uniquement son manifeste `data/processed/source/aisstream/manifests/<session>.json`. Les clés `(source_code,mmsi)` isolent les identifiants des deux sources. MMSI est un émetteur, non une identité permanente ; le suivi est discontinu.
6. Agrégats : `--aggregate data/processed/noaa_candidate/YYYY-MM-ny-nj.parquet` n'accepte **que** le candidat NOAA local ; il revalide l'intégralité du mois et les seuils avant chargement dans `noaa_aggregates`. Le pilote d'une journée est **supprimé**, donc cette table reste vide. Ce chargement ne publie rien.
7. `docker compose stop postgres` pour suspendre la base ; **ne jamais** lancer `docker compose down -v` si les données doivent être conservées.

`sql/analyses.sql` contient cinq requêtes exécutables (pas de vues publiques) : 01 jointure + décompte journalier source ; 02 fenêtre `LAG` et ruptures >6 h ; 03 CTE + classement `DENSE_RANK` des MMSI observés ; 04 qualité et types par source ; 05 rapprochement privé avec un agrégat NOAA candidat éventuel. Les ruptures et changements de MMSI empêchent d'inférer des voyages complets. L'analyse 03 **affiche les MMSI** : ne pas partager ses résultats. `sql/indexes.sql` contient un index optionnel source/temps pour les fenêtres de requête ; `sql/benchmark.sql` reprend la forme de l'analyse 01 sur **une heure UTC** pour une comparaison avant/après.

## Mesures réelles (base du pilote NOAA local)

PostgreSQL **16.15** local, 22 235 observations cargo, 35 MMSI distincts (pas nécessairement 35 navires), une entrée `ingestion_batches`, 0 agrégat, 0 AISStream ; table `observations` **3 792 KiB** (`pg_relation_size`). Premier chargement : **6,92 s** mesurés avec `time.perf_counter` incluant validation des SHA/connexion/transfert ; second chargement : **0,15 s**, 0 insertion et toujours 22 235 lignes. Les cinq requêtes ont été exécutées sur ce lot : respectivement **1, 1, 5, 4, 0** lignes de résultat ; la cinquième est vide parce qu'aucun mois complet n'existe.

`ANALYZE observations` suivi d'un échauffement puis trois `EXPLAIN (ANALYZE, BUFFERS)` identiques sur `sql/benchmark.sql`, même processus/connexion et filtre **2024-01-14 12:00–13:00 UTC**, 983 lignes ; timings *Execution Time* en ms :

| État | Plan sur `observations` | Temps (trois exécutions) | Blocs partagés touchés (premier plan reporté) |
| --- | --- | --- | --- |
| Avant index | Seq Scan ; 21 252 lignes écartées | 1,458 ; 1,443 ; 1,432 | 476 hit |
| Après `sql/indexes.sql` | Bitmap Index Scan + Bitmap Heap Scan ; 983 lignes retenues | 0,559 ; 0,554 ; 0,569 | 29 hit |

Index `observations_source_time_idx` **679 936 octets** ; médianes 1,443 ms vs 0,559 ms (**~2,58× sur cette fenêtre et ce cache**, pas une promesse de gain sur une journée entière ou un autre volume). Cette comparaison n'isole pas tous les effets du cache/de la planification et n'inclut pas le temps de réseau/chargement. Vérifier à nouveau `EXPLAIN` sur le corpus final ; sur une plage couvrant presque toute la table, PostgreSQL peut préférer un Seq Scan.

Tests : `pytest -q` sans DB saute uniquement l'intégration ; pour l'intégration, créer une **base dédiée dont le nom finit en `_test`**, y appliquer `sql/schema.sql`, fournir `TEST_DATABASE_URL` et exécuter `pytest -q` (les tests tronquent les tables de **cette base dédiée**, jamais la base pilote). Ne pas mettre les URL contenant un mot de passe dans Git ou dans un historique de commandes partagé.

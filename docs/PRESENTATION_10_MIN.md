# Présentation du projet — 10 minutes, sans divulguer les données locales

**Personnes à renseigner avant présentation** : [membre 1 : ______], [membre 2 : ______], [membre 3 : ______], [membre 4 : ______], [membre 5 : ______]. Si l'équipe compte moins de cinq personnes, affecter plusieurs créneaux à la même personne ; ne pas inventer de noms. Préparer les diapositives et la démonstration **avec fixtures fictives seulement** ; les requêtes SQL avec MMSI et les points AIS ne doivent pas être projetés ni distribués.

| Temps | Responsable | Slide / message clé | Démonstration sûre |
| --- | --- | --- | --- |
| **0:00–2:00** (2 min) | [membre 1 : ______] | Question et contraintes : cargo AIS 70–79 ≠ porte-conteneurs ; AISStream mondial possible **local seulement**, NOAA limité aux récepteurs US. Présenter la politique et la séparation. | Afficher le schéma ASCII du README, aucune carte de trajets individuels. |
| **2:00–4:00** (2 min) | [membre 2 : ______] | NOAA : collecte asynchrone des archives journalières, timeout/backoff/429, JSONL et reprise ; AISStream : WebSocket 15 min maximum, non capturé faute de clé et sans replay. | Montrer les tests synthétiques, le manifeste **résumé sans ouvrir les lignes JSONL**. Durée NOAA réelle **non mesurée**. |
| **4:00–6:00** (2 min) | [membre 3 : ______] | Transformation en flux/SQLite → Parquet partitionné par source/date, qualité AIS, dédoublonnage, espace de sortie séparé. Suppression de l'agrégat public si mois incomplet/rare ou domination d'un MMSI. | `pytest -q tests/test_transform.py tests/test_publish.py` et résultat `incomplete_month` sur l'unique pilote ; pas de données individuelles. |
| **6:00–8:00** (2 min) | [membre 4 : ______] | PostgreSQL local : tables/contraintes, deux chargements idempotents, 5 requêtes SQL (jointures, CTE, LAG, rang). Index choisi **après** EXPLAIN mesuré, 1,443 → 0,559 ms sur une heure/corpus précis. | Présenter uniquement plans `EXPLAIN`, agrégats chiffrés et comptages des 5 résultats ; **ne pas afficher les MMSI de la requête 03**. |
| **8:00–10:00** (2 min) | [membre 5 : ______] | Bilan et livrable inachevé : 1 jour NOAA, 22 235 points cargo locaux, RAM max transformation 79 968 Kio ; aucun dataset Hub, autorisation et 30 jours manquants, aucun tag v1.0.0. | Carte modèle et garde-fous hors ligne, **pas de compte Hub ni de token à l'écran** ; conclure par prochaines autorisations et limites. |

## Récit d'architecture projetable

```text
     NOAA CSV.zst historique (côte US)         AISStream WebSocket (live, NON capturé)
             | journalier, filtré                 | 15 min max, accès avec clé
             v                                    v
    data/raw/noaa/*.jsonl                  data/raw/aisstream/*.jsonl
             |                                    |
             +---- transformation par lots -------+
                      (source, UTC, qualité)
                             |
               data/processed/source/{noaa,aisstream}/date=.../*.parquet
                             |
               PostgreSQL local (pas de vue publique)
                             |
     NOAA-only, mois entier + seuils --> candidat PRIVE local --> Hub ?
                       janvier incomplet               BLOQUÉ
```

**Attention aux termes** : la flèche `Hub ?` représente une **possibilité non réalisée**. Aucun dépôt Hub, upload, publication publique, capture AISStream ou tag final n'existe. NOAA ne mesure pas toutes les traversées océaniques ; les ruptures de réception empêchent de reconstruire les routes. Les chiffres présentés sont ceux du [seul pilote effectivement mesuré](REPRODUCIBILITY.md), non ceux d'une année ou du monde entier.

## Vérifications avant prise de parole

- Répartir les créneaux avec les vrais noms et faire une répétition chronométrée totalisant 10 minutes.
- Préparer `pytest` synthétique hors ligne et un graphique/tableau **sans MMSI, IMO, nom, latitude, longitude, timestamp précis ou trajet** ; ne jamais ouvrir `data/raw/`, Parquet détaillé, ni la sortie brute SQL contenant des identifiants en projection ou capture d'écran.
- Vérifier `git status`, les fichiers ignorés et la visibilité **privée** du repo GitHub ; ne pas révéler `.env`/tokens. Expliquer que la mise à disposition du code GitHub privé ne confère pas de droit de redistribution des données.
- Dire clairement : durée collecte et RAM ingestion/chargement **non mesurées** ; bénéfice d'index limité au cas testé ; un jour pilote ≠ agrégat public ; Hub bloqué par mois/droits/jeton. Ne pas promettre `v1.0.0` tant que la revue finale manque.

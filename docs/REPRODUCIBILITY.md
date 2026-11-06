# Reproduire le pipeline et vérifier ses limites — bilan au 2026-11-06

**État réel : commits distincts déjà poussés sur le dépôt GitHub privé** [`rsladky/SQL-M1-PIPELINE`](https://github.com/rsladky/SQL-M1-PIPELINE). Une autorisation supplémentaire a permis de collecter/transformer les 30 autres journées NOAA de janvier **en local uniquement**, puis de charger les **31 journées détaillées et l'agrégat candidat dans PostgreSQL local** ; le rapport d'audit reste dans `data/processed/noaa_candidate/2024-01-ny-nj.audit.json` (ignoré par Git). La préparation Hub ne vaut pas dataset publié : aucun repo, URL ou `v1.0.0` Hub ; aucun tag Git `v1.0.0`. Pas de capture AISStream réelle, faute de clé. Aucune présentation n'est demandée ; le scénario de 10 minutes est optionnel. Ne jamais attribuer au projet une collecte historique mondiale complète.

## 1. Chemin sans données ni secrets : installation propre, tests synthétiques

Prérequis : Python 3.11–3.14 (essai sur **3.12.13**), `uv` ou `venv`/`pip`, Git et accès **autorisé** au dépôt privé. Dans un **clone neuf** de ce dépôt, depuis sa racine :

```sh
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e '.[test]'
.venv/bin/python -m pytest -q
```

Alternative avec `python -m venv .venv` puis `.venv/bin/python -m pip install -e '.[test]'` si pip est installé. **Aucune clé ou dataset n'est nécessaire pour ces tests** : le serveur HTTP de test tourne sur la machine locale, les positions sont entièrement fictives, les appels Hub sont simulés et les tests PostgreSQL s'ignorent si `TEST_DATABASE_URL` n'est pas fournie. Contrôle effectif effectué en environnement `/tmp` **neuf**, installation editable de toutes les dépendances : **25 tests réussis, 2 intégrations PostgreSQL ignorées**, en 2,33 s (machine/cache actuels). Le premier essai a révélé une découverte accidentelle de paquets `data/sql` par setuptools ; `pyproject.toml` sélectionne maintenant explicitement `scripts*`. Le wheel construit lors de cet essai historique contenait les modules Python sous `scripts/` et **aucune donnée ni secret** ; ceci ne garantit pas à lui seul l'exécution autonome d'un wheel hors du clone (modèle de carte dans `docs/`).

Pour inclure PostgreSQL dans une **base de test dédiée et jetable** : installer Docker Compose ; préparer votre propre `.env` local, de confiance, compatible shell, mode 0600, avec `POSTGRES_PASSWORD` fort et `DATABASE_URL` locale (voir [guide PostgreSQL](POSTGRES.md)). `docker compose up -d --wait postgres`, puis créer une base **neuve** `ais_pipeline_test` sur ce conteneur :

```sh
docker compose exec -T postgres createdb -U ais_user -O ais_user ais_pipeline_test
(
  . ./.env && export TEST_DATABASE_URL="${DATABASE_URL%/ais_pipeline}/ais_pipeline_test" &&
    .venv/bin/python -m pytest -q
)
```

**Attention : les tests SQL exécutent `TRUNCATE` dans la base `*_test` désignée**. Ne jamais pointer vers une base de travail ou réutiliser une base de test ayant d'autres données. Si `createdb` répond « already exists », inspecter le contenu ou utiliser une nouvelle base jetable avant de rejouer les tests. Le `TEST_DATABASE_URL` doit pointer vers `localhost`/`127.0.0.1` et son nom finir par `_test` ; les tests l'imposent. Avec la base dédiée locale déjà préparée, l'installation `/tmp` neuve a donné **27 tests réussis en 2,11 s** (notamment double chargement, rollback, contraintes, cinq requêtes, filtres NOAA-only et refus de diffusion). Ces durées ne sont pas des benchmarks du pipeline. Nouveau contrôle des tests **sur le clone local actuel (2026-11-06)** : 28 réussis et 2 ignorés sans DB, 30 réussis avec la base `_test` dédiée (mesures dépendantes de cette exécution).

## 2. NOAA réel **déjà local** : reproduction sans nouveau téléchargement

Le clone propre n'inclut **aucune** observation AIS. Les 31 archives quotidiennes officielles de janvier 2024 (dont [celle du 14](https://noaaocm.blob.core.windows.net/ais/csv2/csv2024/ais-2024-01-14.csv.zst)) ont été acquises sous autorisation explicite, filtrées en flux au rectangle NY/NJ **39°–42° N / 75°–71° W**. **Ne pas relancer les téléchargements ni étendre la période sans nouvel accord** ; les JSONL et manifestes existent déjà localement et Git les ignore. Les commandes ci-dessous supposent que les **31** fichiers et manifestes sont disponibles dans `data/raw/noaa/` (un clone Git seul ne suffit pas) :

```sh
for input in data/raw/noaa/2024-01-??-ny-nj.jsonl; do
  .venv/bin/python -m scripts.transform.batches "$input"
done
.venv/bin/python -m scripts.transform.aggregate 2024-01
```

La dernière commande produit maintenant un **candidat exclusivement local**, pas un jeu publiable. Son audit local vérifie chaque archive/manifest/checksum, le type cargo et l'unique zone/mois ; sa diffusion reste bloquée par les droits et par l'audit humain de réidentification/différenciation. Pour **reprendre le chargement local SQL uniquement** (sans nouvelle collecte), vérifier au préalable l'espace disque, le conteneur sain lié à `127.0.0.1`, la `DATABASE_URL` locale et les 31 manifestes/partitions/checksums NOAA. Démarrer PostgreSQL si nécessaire puis exécuter dans un sous-shell qui ne laisse pas le secret exporté ; ne pas supprimer le volume ni vider les tables du pilote :

```sh
(
  . ./.env && export DATABASE_URL &&
    .venv/bin/python -m scripts.load.postgres \
      data/processed/source/noaa/manifests/2024-01-??-ny-nj.json \
      --aggregate data/processed/noaa_candidate/2024-01-ny-nj.parquet
)
```

Cette source `.env` doit être **personnelle et de confiance** : la commande `. ./.env` exécute le fichier. Compose lit `.env` automatiquement, Python non. Sur le mois chargé, une seconde exécution des 31 manifestes insère zéro observation ; l'UPSERT de l'agrégat conserve les valeurs mais peut réécrire la ligne. Les cinq requêtes de `sql/analyses.sql` ont été exécutées sur la base mensuelle et renvoient **1, 31, 5, 17, 1** lignes. **La requête 03 affiche les MMSI : ne jamais capturer, afficher dans un terminal partagé, journaliser, exporter ou publier sa sortie.** Le protocole de mesure sur une heure du 14 reste dans `sql/benchmark.sql`. L'index existe déjà sur la base pilote : ne pas supprimer/recréer cet index pour simuler une comparaison « avant » sur le mois. Voir [mesures et conditions expérimentales](POSTGRES.md).

## 3. Mesures historiques du premier jour pilote (ne pas les extrapoler au mois)

| Métrique | Observation locale | Portée |
| --- | ---: | --- |
| Durée collecte NOAA | **non mesurée** | Téléchargement/ingestion réalisé antérieurement, sans chronométrage ; ne pas l'estimer depuis les tailles. |
| Archive NOAA nationale compressée | **151 376 897 octets** | Une seule journée historique, aucun historique mondial. |
| Lignes de l'archive lues / lignes dans la zone | **5 742 930 / 359 494** | Tous types AIS au stade brut, pas 359 494 cargos. |
| JSONL NOAA local | **193 327 768 octets** | Ignoré par Git ; identifiants et positions sensibles. |
| Transformation sur le lot (Python 3.12/pyarrow 23) | **1,99 s ; pic RSS 79 968 Kio** | `time.perf_counter`/`resource.ru_maxrss` sur un processus, machine/cache locaux ; mémoire collecte/chargement **non mesurée**. |
| Filtre qualité / cargo | **22 235** observations retenues, **334 915** non-cargo, **2 342** invalides, **2** doublons | Total après lecture : 359 494. 35 MMSI distincts détectés, pas 35 navires physiques certifiés. |
| Parquet détaillé NOAA | **501 076 octets** | Une partition source/jour, exclusivement locale. |
| Chargement PostgreSQL pilote | **6,92 s** puis **0,15 s** à la reprise | 22 235 insertions puis **0** ; table `observations` 3 792 KiB ; base locale seulement. |
| Index `(source_code, observed_at)` | **679 936 octets** | Sur **983 points** filtrés en une heure ; médiane `EXPLAIN ANALYZE` **1,443 → 0,559 ms** (~2,58×), cache chaud, pas de garantie sur d'autres périodes/charges. |
| AISStream réel / agrégat Hub | **aucun / aucun** | Pas de clé ; candidat mensuel local désormais présent mais droits Hub non validés, donc **pas d'URL**. |

La durée de collecte du **premier jour historique** n'avait pas été mesurée. Pour l'extension aux 30 journées, un premier processus a expiré au bout de **2 400 s** après avoir achevé 19 nouvelles journées (plus le jour 14 réutilisé) ; le second processus a achevé les 11 dernières en **921,22 s**. Le temps total exact et le trafic réseau réellement transféré (téléchargements partiels repris) ne sont **pas mesurés** ; ne pas additionner les temps comme une durée exhaustive. La transformation des 31 jours et la génération du candidat ont été chronométrées **localement** dans le rapport d'audit ignoré par Git. Aucune RAM de collecte complète/chargement mensuel, aucune vitesse AISStream et aucun gain d'index sur un corpus mensuel ne sont connus. Les données NOAA proviennent de récepteurs côtiers US, sont filtrées à la minute et présentent des trous ; les codes 70–79 ne distinguent pas les porte-conteneurs. L'agrégat sûr nécessite un **mois entier**, ≥20 MMSI, ≥200 observations, ≥7 jours observés et aucun MMSI >10 % des observations ; ces seuils ne prouvent pas à eux seuls la non-réidentification. Voir [politique détaillée](DATA_POLICY.md).

## 3 bis. Chargement SQL du mois (2026-11-06, exclusivement local)

Prévol en lecture seule : **98 Go disponibles**, conteneur PostgreSQL `running/healthy` lié à **127.0.0.1:5432**, URL de la base pilote sur boucle locale (secret non affiché), **31** manifestes NOAA journaliers et checksums/partitions vérifiés, candidat mensuel cohérent. Sur la base pilote conservée, le chargeur transactionnel a sauté le jour déjà présent et inséré **760 368** observations supplémentaires en **68,94 s** (client, vérification des SHA et connexion comprises) ; revalidation et chargement de l'agrégat : **89,93 s** au total. Après contrôle SQL : **31 lots** et dates, **782 603** observations NOAA détaillées, dont **51** exclues par qualité pour l'agrégation ; **782 552** observations admises et **315** émetteurs MMSI distincts admis, en accord avec la ligne de `noaa_aggregates` et le Parquet candidat. **0** observation AISStream ; aucune exportation. Rejouer les 31 manifestes insère **0** observation en **3,73 s** ; les effectifs SQL restent stables après une seconde revalidation de l'agrégat (**26,63 s** pour la reprise complète). Aucune de ces valeurs ne constitue un accord de diffusion ni un benchmark transposable.

Les cinq requêtes SQL renvoient maintenant **1, 31, 5, 17, 1** lignes (la requête 05 confronte l'agrégat réellement présent) ; **ne pas divulguer** les résultats individuels de 03. L'index source/temps est effectivement utilisé sur l'heure du 14 (**983** points ; trois `EXPLAIN ANALYZE` à cache chaud : **0,585 / 0,581 / 0,576 ms**) ; un comptage du mois complet privilégie au contraire le parcours séquentiel parallélisé (**35,454 / 30,760 / 35,680 ms**). Ce n'est **pas** une comparaison avant/après index sur le mois : l'index existait déjà. Le protocole, les tailles et le tableau **historique sur une seule journée** sont distingués dans [POSTGRES.md](POSTGRES.md).

## 4. Audit des données hors Git (rejouer localement)

```sh
git status --short
git ls-files data
git log --all --pretty=format: --name-only | grep '^data/' | sort -u
git rev-list --objects --all | grep -Ei '(\.env$|\.jsonl|\.parquet|\.csv|\.zst|checkpoint|manifest)' || true
git check-ignore -v .env data/raw/noaa/2024-01-14-ny-nj.jsonl \
  data/processed/source/noaa/date=2024-01-14/2024-01-14-ny-nj.parquet
git diff --check
```

À la date du présent audit, `git ls-files data` et l'historique des noms ne montrent **que** `data/raw/.gitkeep` et `data/processed/.gitkeep` ; aucune archive, position, clé ou secret n'a été trouvée dans les fichiers suivis lors de l'audit de noms/motifs connus. Un simple grep n'établit **pas** l'absence absolue de tout secret inconnu : relire le diff et les fichiers ajoutés avant le push, surtout l'exemple `.env.example`. GitHub confirme un dépôt **PRIVATE**, pas une autorisation de redistribution AIS. Les fichiers de tests sont synthétiques, les éventuels droits NOAA sont à revérifier avant un upload privé.

## 5. Conditions pour déclarer le projet complet — **non satisfaites**

- **La collecte des 30 journées NOAA manquantes a été explicitement autorisée puis achevée localement.** Pour une autre période, demander une nouvelle autorisation. Une clé AISStream reste absente ; n'effectuer une courte capture que si elle est fournie et que les conditions permettent le stockage local. Mesurer ses temps/RAM/volumes si elle a lieu.
- Vérifier humainement la FAQ NOAA (produits dérivés avec citation et restriction USCG), la notice du produit **CSV.zst 2024 exact**, l'usage de planification côtière et les risques de non-réidentification/différenciation ; consigner localement dates, versions, URL, interprétation, incertitude et choix du propriétaire. Si doute non levé, suspendre l'upload et demander éventuellement une clarification écrite à NOAA : une lettre individuelle n'est **pas prétendue** par la voie d'attestation documentaire. La revue n'est ni avis juridique ni autorisation d'upload ; créer manuellement un dépôt Hub dataset **privé et vide seulement sous nouvel accord**, puis un éventuel upload séparé avec jeton hors Git. [Procédure bloquante](PUBLISHING.md).
- Décider explicitement d'une éventuelle ouverture publique et d'une licence/notice correctement étayée ; documenter citation, URL et version du dataset réel si autorisées. Le code actuel **ne rend rien public** et n'émet aucun tag Hub.
- Des commits/pushs distincts sont présents, mais un nouveau contrôle des documents, tests, état Git et historique est nécessaire avant toute action supplémentaire. **Ne pas créer ni pousser de tag Git `v1.0.0`** tant que les critères juridiques et de livrable final ci-dessus ne sont pas remplis.

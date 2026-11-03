# Reproduire le pipeline et vérifier ses limites — bilan au 2026-11-03

**État réel : cinq commits distincts déjà poussés sur le dépôt GitHub privé** [`rsladky/SQL-M1-PIPELINE`](https://github.com/rsladky/SQL-M1-PIPELINE) ; l'étape 6 sera un sixième commit **après revue par l'utilisateur**. La préparation Hub ne vaut pas dataset publié : aucun repo, URL ou `v1.0.0` Hub ; aucun tag Git `v1.0.0`. Pas de capture AISStream réelle, faute de clé. Ne jamais attribuer au projet une collecte historique mondiale complète.

## 1. Chemin sans données ni secrets : installation propre, tests synthétiques

Prérequis : Python 3.11–3.14 (essai sur **3.12.13**), `uv` ou `venv`/`pip`, Git et accès **autorisé** au dépôt privé. Dans un **clone neuf** de ce dépôt, depuis sa racine :

```sh
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e '.[test]'
.venv/bin/python -m pytest -q
```

Alternative avec `python -m venv .venv` puis `.venv/bin/python -m pip install -e '.[test]'` si pip est installé. **Aucune clé ou dataset n'est nécessaire pour ces tests** : le serveur HTTP de test tourne sur la machine locale, les positions sont entièrement fictives, les appels Hub sont simulés et les tests PostgreSQL s'ignorent si `TEST_DATABASE_URL` n'est pas fournie. Contrôle effectif effectué en environnement `/tmp` **neuf**, installation editable de toutes les dépendances : **25 tests réussis, 2 intégrations PostgreSQL ignorées**, en 2,33 s (machine/cache actuels). Le premier essai a révélé une découverte accidentelle de paquets `data/sql` par setuptools ; `pyproject.toml` sélectionne maintenant explicitement `scripts*`. Le wheel construit localement contient six modules Python sous `scripts/` et **aucune donnée ni secret** ; ceci ne garantit pas à lui seul l'exécution autonome d'un wheel hors du clone (modèle de carte dans `docs/`).

Pour inclure PostgreSQL dans une **base de test dédiée et jetable** : installer Docker Compose ; préparer votre propre `.env` local, de confiance, compatible shell, mode 0600, avec `POSTGRES_PASSWORD` fort et `DATABASE_URL` locale (voir [guide PostgreSQL](POSTGRES.md)). `docker compose up -d --wait postgres`, puis créer une base **neuve** `ais_pipeline_test` sur ce conteneur :

```sh
docker compose exec -T postgres createdb -U ais_user -O ais_user ais_pipeline_test
(
  . ./.env && export TEST_DATABASE_URL="${DATABASE_URL%/ais_pipeline}/ais_pipeline_test" &&
    .venv/bin/python -m pytest -q
)
```

**Attention : les tests SQL exécutent `TRUNCATE` dans la base `*_test` désignée**. Ne jamais pointer vers une base de travail ou réutiliser une base de test ayant d'autres données. Si `createdb` répond « already exists », inspecter le contenu ou utiliser une nouvelle base jetable avant de rejouer les tests. Le `TEST_DATABASE_URL` doit pointer vers `localhost`/`127.0.0.1` et son nom finir par `_test` ; les tests l'imposent. Avec la base dédiée locale déjà préparée, l'installation `/tmp` neuve a donné **27 tests réussis en 2,11 s** (notamment double chargement, rollback, contraintes, cinq requêtes, filtres NOAA-only et refus de diffusion). Ces durées ne sont pas des benchmarks du pipeline.

## 2. Pilote NOAA réel **déjà local** : reproduction uniquement sur autorisation de collecte

Le clone propre n'inclut **aucune** observation AIS. Le pilote autorisé existant vient de l'archive officielle [NOAA 2024-01-14 CSV.zst](https://noaaocm.blob.core.windows.net/ais/csv2/csv2024/ais-2024-01-14.csv.zst), nationale et filtrée en flux au rectangle NY/NJ **39°–42° N / 75°–71° W**. **Ne pas relancer le téléchargement sans nouvel accord** : le lot (~151 Mo compressés) existe déjà localement, Git l'ignore ; l'ingestion refuse moins de 3 Go libres. Les commandes ci-dessous supposent qu'il existe dans `data/raw/noaa/` et que ses manifestes sont intacts :

```sh
.venv/bin/python -m scripts.transform.batches data/raw/noaa/2024-01-14-ny-nj.jsonl
.venv/bin/python -m scripts.transform.aggregate 2024-01
```

La seconde commande renvoie **`incomplete_month`, 30 jours manquants**, sans candidat publiable. Pour le chargement local SQL uniquement, démarrer PostgreSQL comme ci-dessus et exécuter dans un sous-shell qui ne laisse pas le secret exporté :

```sh
(
  . ./.env && export DATABASE_URL &&
    .venv/bin/python -m scripts.load.postgres \
      data/processed/source/noaa/manifests/2024-01-14-ny-nj.json
)
```

Cette source `.env` doit être **personnelle et de confiance** : la commande `. ./.env` exécute le fichier. Compose lit `.env` automatiquement, Python non. Le second chargement insère zéro ligne si les fichiers ne changent pas. Les cinq requêtes sont dans `sql/analyses.sql` et ont été exécutées sur le pilote ; pour une démonstration strictement **locale** avec `psql` et sans secret dans l'historique : `docker compose exec -T postgres psql -U ais_user -d ais_pipeline < sql/analyses.sql`. **La requête 03 affiche les MMSI : ne pas capturer, partager ou publier sa sortie.** `sql/benchmark.sql` et `sql/indexes.sql` permettent de reproduire un EXPLAIN avant/après sur une base neuve **avant** création de l'index ; le volume actuel a déjà l'index, donc ne pas prétendre refaire une comparaison « avant » sur cet état. Voir [mesures et conditions expérimentales](POSTGRES.md).

## 3. Chiffres du seul lot pilote (mesurés, pas extrapolés)

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
| AISStream réel / agrégat Hub | **aucun / aucun** | Pas de clé ; janvier incomplet ; conditions Hub non validées, donc **pas de mesure ni URL**. |

Aucune durée de collecte NOAA, aucune RAM de collecte/chargement, aucune vitesse AISStream et aucun gain d'index sur un corpus complet ne sont connus. Les données NOAA proviennent de récepteurs côtiers US, sont filtrées à la minute et présentent des trous ; les codes 70–79 ne distinguent pas les porte-conteneurs. L'agrégat sûr nécessite un **mois entier**, ≥20 MMSI, ≥200 observations, ≥7 jours observés et aucun MMSI >10 % des observations ; ces seuils ne prouvent pas à eux seuls la non-réidentification. Voir [politique détaillée](DATA_POLICY.md).

## 4. Audit avant le sixième commit/push (rejouer localement)

```sh
git status --short
git ls-files data
git log --all --pretty=format: --name-only | grep '^data/' | sort -u
git rev-list --objects --all | grep -Ei '(\.env$|\.jsonl|\.parquet|\.csv|\.zst|checkpoint|manifest)' || true
git check-ignore -v .env data/raw/noaa/2024-01-14-ny-nj.jsonl \
  data/processed/source/noaa/date=2024-01-14/2024-01-14-ny-nj.parquet
git diff --check
```

Avant l'étape 6, `git ls-files data` et l'historique des noms ne montrent **que** `data/raw/.gitkeep` et `data/processed/.gitkeep` ; aucune archive, position, clé ou secret n'a été trouvée dans les fichiers suivis lors de l'audit de noms/motifs connus. Un simple grep n'établit **pas** l'absence absolue de tout secret inconnu : relire le diff et les fichiers ajoutés avant le push, surtout l'exemple `.env.example`. GitHub confirme un dépôt **PRIVATE**, pas une autorisation de redistribution AIS. Les fichiers de tests sont synthétiques, les éventuels droits NOAA sont à revérifier avant un upload privé.

## 5. Conditions pour déclarer le projet complet — **non satisfaites**

- Obtenir **une nouvelle autorisation** pour la collecte des 30 jours NOAA manquants et, séparément, une clé AISStream ; réaliser une courte capture seulement si la clé est fournie et les conditions d'usage permettent le stockage local. Mesurer alors les temps/RAM/volumes réellement obtenus.
- Vérifier la notice du produit NOAA 2024 précis et la portée d'un dépôt Hub privé/public ; si doute, demander une réponse écrite à NOAA. Évaluer non-réidentification et effets de comparaison entre versions. Créer le dépôt Hub dataset privé **seulement sous nouvel accord** ; ne téléverser qu'un agrégat NOAA complet, après audit des droits, avec jeton hors Git. [Procédure bloquante](PUBLISHING.md).
- Décider explicitement d'une éventuelle ouverture publique et d'une licence/notice correctement étayée ; documenter citation, URL et version du dataset réel si autorisées. Le code actuel **ne rend rien public** et n'émet aucun tag Hub.
- Faire relire les documents, tests, état Git et historique ; seul le parent/orchestrateur peut faire le **sixième commit/push** après validation. **Ne pas créer ni pousser de tag Git `v1.0.0`** tant que les critères juridiques et de livrable final ci-dessus ne sont pas remplis.

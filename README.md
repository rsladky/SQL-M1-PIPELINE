# Pipeline AIS cargos — pilote local, publication Hub bloquée

**Statut : commits distincts poussés sur GitHub ; les 31 jours NOAA détaillés et l'agrégat candidat de janvier sont chargés dans PostgreSQL local, le paquet Dataset Card reste local, publication Hub et validation juridique toujours bloquées. Aucun tag `v1.0.0`.** Aucun dépôt ni téléversement Hub. Le candidat local ne vaut **pas** autorisation de publication. AISStream n'a pas été contacté faute de clé. Le dépôt reste privé. Voir [provenance, limites et règles de diffusion](docs/DATA_POLICY.md), [reproductibilité et chiffres vérifiés](docs/REPRODUCIBILITY.md) ; le [scénario de 10 minutes](docs/PRESENTATION_10_MIN.md) est facultatif et **aucune présentation n'est demandée**.

## Architecture locale (publication future non opérationnelle)

```text
NOAA (archive historique CSV.zst) ──┐
                                    ├─> data/raw/<source>/ JSONL ─> data/processed/<source>/ Parquet ─> PostgreSQL local
AISStream (WebSocket direct) ───────┘                       └─> agrégats NOAA-only contrôlés ─> Hub (privé jusqu'à validation)
```

Les sources conservent des provenance et espaces de stockage séparés. AISStream reste exclusivement local et n'alimente **jamais** le jeu public. NOAA ne couvre pas les traversées mondiales ; le code AIS 70–79 indique « cargo », pas spécifiquement « porte-conteneurs ».

## Préparer l'environnement et collecter **localement**

- Python **3.11 à 3.14**, gestionnaire d'environnement (`venv`/`pip` ou `uv`). Installer les dépendances : `python -m venv .venv && .venv/bin/python -m pip install -e '.[test]'` (installer pip au besoin), puis `.venv/bin/python -m pytest -q`. Alternative ciblée : `uv venv .venv && uv pip install --python .venv/bin/python 'aiohttp>=3.10,<4' 'zstandard>=0.23,<1' 'pyarrow>=17,<24' 'psycopg[binary]>=3.2,<4' 'huggingface-hub>=0.30,<2' 'pytest>=8,<10'` pour tous les tests hors PostgreSQL.
- Vérifier l'espace disque avant l'archive nationale : `df -h .` ; l'ingestion refuse moins de **3 Go libres** et des archives compressées de plus de **1 Go**. Exécuter `.venv/bin/python -m scripts.ingest.noaa --date 2024-01-14` depuis la racine du dépôt. Source : archive NOAA quotidienne officielle CSV.zst nationale ; filtrage **à la lecture** du rectangle NY/NJ, sans chargement complet en RAM. Sortie `data/raw/noaa/2024-01-14-ny-nj.jsonl` et manifeste local. Une nouvelle exécution vérifie taille et SHA-256 avant de réutiliser le lot ; en cas d'interruption, le lot incomplet est reconstruit atomiquement. Téléchargements multi-jours internes limités à trois connexions ; HTTP 429/5xx et délais réessayés avec backoff. La taille max du JSONL est **2 Go**.
- AISStream **seulement si une clé personnelle est fournie** dans l'environnement : `AISSTREAM_API_KEY=... .venv/bin/python -m scripts.ingest.aisstream --duration 900 --session-id pilote-1` (ne pas écrire la valeur de la clé dans le terminal partagé, l'historique shell ou Git ; charger depuis un gestionnaire de secrets). Même identifiant pour reprendre une session interrompue, dans la limite des **15 minutes calendaires initiales** ; sinon nouvel identifiant. Une capture crée `data/raw/aisstream/<id>.jsonl` et un manifeste local ; les messages de position sans classification cargo statique préalable ne sont pas retenus. Une reconnexion **ne rejoue pas** les observations perdues ; déduplication locale à fenêtre bornée. La clé n'est pas disponible ici : aucun appel WebSocket réel n'a eu lieu.
- Pour PostgreSQL : Docker Compose et variables `POSTGRES_PASSWORD`/`DATABASE_URL` dans un `.env` **local et ignoré**, jamais dans Git. Voir [guide PostgreSQL et mesures](docs/POSTGRES.md). `HF_TOKEN` n'est pas disponible et ne doit pas être ajouté au dépôt. Les données NOAA/AISStream, manifests, checkpoints et fichiers temporaires sont exclus de Git par `.gitignore`. Le mois est complet localement, mais `scripts/publish/private_hub.py` reste bloqué faute d'attestation humaine documentée des textes officiels/risques, de jeton et d'accord **séparé** d'upload ; aucune permission individuelle NOAA n'est prétendue.

**Premier pilote NOAA historique du 2024-01-14** : archive compressée **151 376 897 octets**, **5 742 930 lignes** lues, **359 494** observations dans le rectangle, **0** ligne invalide, JSONL **193 327 768 octets** (manifeste local ignoré par Git). Ce sont des observations de tous types, **pas un compte de cargos** avant transformation. Par autorisation ultérieure, les 30 autres jours de janvier ont été téléchargés et transformés **en local seulement** ; leurs volumes/audit restent dans `data/processed/noaa_candidate/2024-01-ny-nj.audit.json` ignoré par Git. Aucune donnée de ce lot ne doit être ajoutée au dépôt.

## Transformer et contrôler un agrégat candidat **local**

- Depuis la racine : `.venv/bin/python -m scripts.transform.batches data/raw/noaa/2024-01-14-ny-nj.jsonl`. Parquet détaillé partitionné sous `data/processed/source/noaa/date=2024-01-14/`, manifeste de reprise dans `data/processed/source/noaa/manifests/`. Un fichier AISStream **terminé** se traite séparément : `.venv/bin/python -m scripts.transform.batches data/raw/aisstream/<session-id>.jsonl` ; jamais mélanger les sources. Seuls les types 70–79 sont conservés. Le filtre de validité, les sentinelles AIS, la déduplication exacte et les drapeaux d'écarts de position/trous sont locaux ; MMSI n'est pas une identité permanente. Le JSONL se lit ligne par ligne, SQLite temporaire dédoublonne sur disque, PyArrow écrit par paquets de 5 000 lignes. Les sorties détaillées contiennent des identifiants et positions : **jamais publiques**.
- `.venv/bin/python -m scripts.transform.aggregate 2024-01` demande un **mois entier** de 31 fichiers NOAA journaliers validés, dans la seule zone large NY/NJ. Seuls les mois complets avec ≥20 MMSI distincts, ≥200 observations validées et ≥7 jours effectivement observés, sans navire dépassant 10 % des observations, produisent un Parquet candidat `data/processed/noaa_candidate/`. Le mois entier a été validé et un candidat **local uniquement** existe désormais ; la revue documentaire des droits applicables et des risques de divulgation reste à effectuer. Le test synthétique de mois incomplet vérifie toujours le refus. Un candidat n'est pas une autorisation de publication ; relire conditions NOAA, risques de reconstruction et versionnement avant toute mise en ligne. Les sorties candidates ne portent ni identifiant, ni position, ni horaire précis.
- **Mesure du lot local, transformation à froid** (2026-11-03, Python 3.12/pyarrow 23, `time.perf_counter` + `resource.ru_maxrss`) : **1,99 s**, **79 968 Kio RSS max**, 359 494 lignes entrantes, 22 235 observations cargo valides, 334 915 non-cargo, 2 342 invalides, 2 doublons ; 1 Parquet local de **501 076 octets**. Ces chiffres dépendent de la machine/cache et **n'incluent pas** ingestion, PostgreSQL ni AISStream ; les 35 MMSI distincts ne sont pas un dénombrement certain de navires. Aucun Parquet de données ne doit être ajouté au dépôt.

## PostgreSQL et analyses **locales**

La base PostgreSQL 16 du `compose.yaml` lie uniquement `127.0.0.1`. Avant chargement : vérifier `df -h .`, `docker compose ps` (service sain et port lié à la boucle locale), le nom/host de `DATABASE_URL` hors Git et les **31** manifestes et checksums NOAA. **Compose lit `.env` automatiquement, pas le script Python** ; sourcer uniquement son propre `.env` de confiance dans un sous-shell, jamais afficher le secret. Le chargeur transactionnel/idempotent reprend les jours déjà chargés sans réinitialiser la base :

```sh
(
  . ./.env && export DATABASE_URL &&
    .venv/bin/python -m scripts.load.postgres \
      data/processed/source/noaa/manifests/2024-01-??-ny-nj.json \
      --aggregate data/processed/noaa_candidate/2024-01-ny-nj.parquet
)
```

Au **2026-11-06**, `ingestion_batches` contient **31** lots NOAA pour janvier ; `observations`, **782 603** points NOAA dont **51** exclus par qualité de l'agrégat ; `noaa_aggregates`, **une** ligne locale (315 émetteurs MMSI distincts, **782 552** observations retenues). Aucune observation AISStream. Le second chargement des 31 manifestes insère **0** point. Les chiffres SQL sont cohérents avec le candidat ; l'agrégat n'est **pas** un dataset publié ni une autorisation de diffusion. [Détail des conditions, cinq requêtes et mesures d'index](docs/POSTGRES.md) ; ne jamais afficher/partager les résultats SQL contenant MMSI ou positions.

## Hub — préparation **sans publication**

[Procédure, interprétation des droits et garde-fous](docs/PUBLISHING.md) ; [modèle de Dataset Card](docs/DATASET_CARD_TEMPLATE.md). Le garde-fou accepte désormais une **attestation locale non vide** d'une revue humaine datée de la FAQ NOAA (produits dérivés avec citation **et** restriction USCG), de la notice applicable au **CSV.zst 2024 exact**, de l'usage de planification côtière et des risques de réidentification/différenciation. Cette auto-attestation est une interprétation à risque assumé, **ni permission individuelle NOAA ni avis juridique** ; elle ne confère aucune licence CSV CC0. Le script reste **hors ligne par défaut**, n'implémente ni création de repo, ni ouverture publique, ni tag ; toute opération future exige un **nouvel accord distinct**, un jeton local et un dépôt dataset **préexistant, PRIVATE et vide**. ID envisagé : `kinkin404/noaa-ais-cargo-ny-nj-2024-01`, non créé ni vérifié à distance. Aucune attestation réelle, aucun token ni upload existent actuellement.

## Dataset agrégé et carte **brouillons locaux uniquement**

Commande depuis la racine : `.venv/bin/python -m scripts.publish.local_package 2024-01`. Elle vérifie **en lecture seule** les 31 provenances/manifestes/checksums NOAA, le schéma limité à quatre colonnes, une seule ligne, les seuils et les contributions, puis copie uniquement le Parquet agrégé déjà présent dans `data/processed/noaa_candidate/` vers `data/local_dataset/noaa-2024-01-ny-nj/data/noaa_aggregate_2024-01.parquet`. Elle rend `data/local_dataset/noaa-2024-01-ny-nj/README.md` depuis [le modèle de carte locale](docs/LOCAL_DATASET_CARD_TEMPLATE.md) et écrit `manifest.json` avec les SHA-256 ; une seconde exécution vérifie le paquet sans l'écraser. Le candidat original reste intact. Le dossier entier est **ignoré par Git**, ne contient ni point AIS individuel, ni identifiant, ni trace ou secret et n'utilise **aucun réseau**. La carte indique « **DRAFT LOCAL ONLY — NOT PUBLISHED** », cite prudemment NOAA CSV.zst et précise qu'aucune licence/permission de redistribution pour ce produit n'est établie. **Ne pas ajouter ce dossier à Git ni le téléverser manuellement au Hub.** Le garde-fou distinct `scripts.publish.private_hub` exige maintenant une **revue documentaire humaine locale explicitement attestée** et un **nouvel accord d'upload** ; la carte brouillon déjà présente ne les fournit pas et ne doit jamais être téléversée directement.

## Étapes envisagées, **sur validation distincte de l'utilisateur**

1. **Terminée** : cadrage, dépôt privé et configuration locale.
2. **Implémentée, avec pilote NOAA local** : collecte asynchrone à reprise vers `data/raw/` JSONL ; tests de panne synthétiques. Capture AISStream réelle bloquée faute de clé.
3. **Implémentée, janvier NOAA transformé localement** : Parquet par source/date et candidat NOAA-only local ; mois incomplet toujours supprimé par les contrôles, tests synthétiques.
4. **Implémentée et chargée en local** : PostgreSQL contient les 31 jours NOAA et l'agrégat candidat ; reprise idempotente, cinq analyses SQL, ancien `EXPLAIN ANALYZE` avant/après index sur le pilote et nouveau plan sur le mois complet (sans fausse comparaison avant/après).
5. **Préparation/tests uniquement** : garde-fou privé avec revue humaine locale des textes officiels/risques et Dataset Card prudente ; aucune attestation réelle, aucun dépôt ni fichier envoyé. Malgré le mois complet local, jeton, notice exacte revérifiée, décision humaine et accord **distinct** d'upload restent absents.
6. **Documentation disponible, validation juridique/finale en attente** : [guide de reproduction](docs/REPRODUCIBILITY.md), mesures du pilote et du mois local, audit Git ; [scénario 10 minutes](docs/PRESENTATION_10_MIN.md) conservé **facultativement**, aucune présentation requise. Tag final uniquement une fois les droits et le dataset validés.

La publication réelle de l'étape 5 et le livrable final/tag de l'étape 6 ne sont **pas** faits. Aucune capture AISStream réelle ni URL Hub n'est disponible. Les critères de publication restent à confirmer dans `docs/DATA_POLICY.md` : janvier est complet **en local**, mais le candidat ne peut pas être transféré au Hub dans l'état actuel des droits et garde-fous.

## Références

- [Marine Cadastre AIS](https://marinecadastre.gov/ais/) ; [FAQ et conditions d'usage NOAA](https://coast.noaa.gov/data/marinecadastre/ais/faq.pdf) ; [index du produit NOAA 2024](https://noaaocm.blob.core.windows.net/ais/csv2/csv2024/index.html).
- [AISStream : protocole, limites, accès](https://aisstream.io/documentation).
- [Dataset Cards Hugging Face](https://huggingface.co/docs/hub/datasets-cards).

Consulter la politique détaillée **avant** tout téléchargement ou téléversement ; aucune licence de données n'est présumée.

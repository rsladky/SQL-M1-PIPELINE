# Pipeline AIS cargos — étapes 1 à 4 : pipeline local jusqu'à PostgreSQL

**Statut : ingestion, transformation et chargement/analyse PostgreSQL locaux implémentés et testés ; publication non implémentée.** Le pilote NOAA reste exclusivement local : son unique journée ne produit aucun agrégat publiable. AISStream n'a pas été contacté faute de clé. Le dépôt reste privé. Voir [provenance, limites et règles de diffusion](docs/DATA_POLICY.md).

## Architecture locale (publication future non opérationnelle)

```text
NOAA (archive historique CSV.zst) ──┐
                                    ├─> data/raw/<source>/ JSONL ─> data/processed/<source>/ Parquet ─> PostgreSQL local
AISStream (WebSocket direct) ───────┘                       └─> agrégats NOAA-only contrôlés ─> Hub (privé jusqu'à validation)
```

Les sources conservent des provenance et espaces de stockage séparés. AISStream reste exclusivement local et n'alimente **jamais** le jeu public. NOAA ne couvre pas les traversées mondiales ; le code AIS 70–79 indique « cargo », pas spécifiquement « porte-conteneurs ».

## Préparer l'environnement et collecter **localement**

- Python **3.11 à 3.14**, gestionnaire d'environnement (`venv`/`pip` ou `uv`). Installer les dépendances : `python -m venv .venv && .venv/bin/python -m pip install -e '.[test]'` (installer pip au besoin), puis `.venv/bin/python -m pytest -q`. Alternative ciblée : `uv venv .venv && uv pip install --python .venv/bin/python 'aiohttp>=3.10,<4' 'zstandard>=0.23,<1' 'pyarrow>=17,<24' 'pytest>=8,<10'` pour l'ingestion/la transformation/tests.
- Vérifier l'espace disque avant l'archive nationale : `df -h .` ; l'ingestion refuse moins de **3 Go libres** et des archives compressées de plus de **1 Go**. Exécuter `.venv/bin/python -m scripts.ingest.noaa --date 2024-01-14` depuis la racine du dépôt. Source : archive NOAA quotidienne officielle CSV.zst nationale ; filtrage **à la lecture** du rectangle NY/NJ, sans chargement complet en RAM. Sortie `data/raw/noaa/2024-01-14-ny-nj.jsonl` et manifeste local. Une nouvelle exécution vérifie taille et SHA-256 avant de réutiliser le lot ; en cas d'interruption, le lot incomplet est reconstruit atomiquement. Téléchargements multi-jours internes limités à trois connexions ; HTTP 429/5xx et délais réessayés avec backoff. La taille max du JSONL est **2 Go**.
- AISStream **seulement si une clé personnelle est fournie** dans l'environnement : `AISSTREAM_API_KEY=... .venv/bin/python -m scripts.ingest.aisstream --duration 900 --session-id pilote-1` (ne pas écrire la valeur de la clé dans le terminal partagé, l'historique shell ou Git ; charger depuis un gestionnaire de secrets). Même identifiant pour reprendre une session interrompue, dans la limite des **15 minutes calendaires initiales** ; sinon nouvel identifiant. Une capture crée `data/raw/aisstream/<id>.jsonl` et un manifeste local ; les messages de position sans classification cargo statique préalable ne sont pas retenus. Une reconnexion **ne rejoue pas** les observations perdues ; déduplication locale à fenêtre bornée. La clé n'est pas disponible ici : aucun appel WebSocket réel n'a eu lieu.
- Pour PostgreSQL : Docker Compose et variables `POSTGRES_PASSWORD`/`DATABASE_URL` dans un `.env` **local et ignoré**, jamais dans Git. Voir [guide PostgreSQL et mesures](docs/POSTGRES.md). `HF_TOKEN` est réservé à une future étape. Les données NOAA/AISStream, manifests, checkpoints et fichiers temporaires sont exclus de Git par `.gitignore`. Seul `scripts/publish/` attend une étape ultérieure.

**Pilote NOAA réalisé localement** : archive compressée **151 376 897 octets**, **5 742 930 lignes** lues, **359 494** observations dans le rectangle, **0** ligne invalide, JSONL **193 327 768 octets** (manifeste local ignoré par Git). Ce sont des observations de tous types, **pas un compte de cargos** avant transformation. Aucun fichier de ce lot ne doit être ajouté au dépôt.

## Transformer et contrôler un agrégat candidat **local**

- Depuis la racine : `.venv/bin/python -m scripts.transform.batches data/raw/noaa/2024-01-14-ny-nj.jsonl`. Parquet détaillé partitionné sous `data/processed/source/noaa/date=2024-01-14/`, manifeste de reprise dans `data/processed/source/noaa/manifests/`. Un fichier AISStream **terminé** se traite séparément : `.venv/bin/python -m scripts.transform.batches data/raw/aisstream/<session-id>.jsonl` ; jamais mélanger les sources. Seuls les types 70–79 sont conservés. Le filtre de validité, les sentinelles AIS, la déduplication exacte et les drapeaux d'écarts de position/trous sont locaux ; MMSI n'est pas une identité permanente. Le JSONL se lit ligne par ligne, SQLite temporaire dédoublonne sur disque, PyArrow écrit par paquets de 5 000 lignes. Les sorties détaillées contiennent des identifiants et positions : **jamais publiques**.
- `.venv/bin/python -m scripts.transform.aggregate 2024-01` demande un **mois entier** de 31 fichiers NOAA journaliers validés, dans la seule zone large NY/NJ. Seuls les mois complets avec ≥20 MMSI distincts, ≥200 observations validées et ≥7 jours effectivement observés, sans navire dépassant 10 % des observations, produisent un Parquet candidat `data/processed/noaa_candidate/`. Le pilote renvoie `incomplete_month` (30 jours manquants) et **n'écrit aucun agrégat candidat**. Un candidat n'est pas une autorisation de publication ; relire conditions NOAA, risques de reconstruction et versionnement avant toute mise en ligne. Les sorties candidates ne portent ni identifiant, ni position, ni horaire précis.
- **Mesure du lot local, transformation à froid** (2026-11-03, Python 3.12/pyarrow 23, `time.perf_counter` + `resource.ru_maxrss`) : **1,99 s**, **79 968 Kio RSS max**, 359 494 lignes entrantes, 22 235 observations cargo valides, 334 915 non-cargo, 2 342 invalides, 2 doublons ; 1 Parquet local de **501 076 octets**. Ces chiffres dépendent de la machine/cache et **n'incluent pas** ingestion, PostgreSQL ni AISStream ; les 35 MMSI distincts ne sont pas un dénombrement certain de navires. Aucun Parquet de données ne doit être ajouté au dépôt.

## PostgreSQL et analyses **locales**

La base PostgreSQL 16 du `compose.yaml` lie uniquement `127.0.0.1`. Après configuration du secret hors Git, `docker compose up -d --wait postgres`. **Compose lit `.env` automatiquement, pas le script Python** : depuis la racine, charger uniquement votre propre `.env` de confiance (affectations compatibles avec le shell), sans afficher de secret ni le saisir dans l'historique : `(. ./.env && export DATABASE_URL && .venv/bin/python -m scripts.load.postgres data/processed/source/noaa/manifests/2024-01-14-ny-nj.json)`. Le sous-shell évite de conserver `DATABASE_URL` exportée dans la session. Voir [le détail de cette précaution](docs/POSTGRES.md). `sql/schema.sql` crée les tables `sources`, `vessels`, `observations`, `ingestion_batches` et `noaa_aggregates` ; `sql/analyses.sql` documente cinq requêtes exécutables. Le chargement valide la provenance et les checksums et reste transactionnel/idempotent : **22 235 observations au premier chargement (6,92 s), 0 au second (0,15 s)**. L'agrégat NOAA demeure vide, faute de mois complet. [Détail des contraintes, requêtes et `EXPLAIN ANALYZE` avant/après index](docs/POSTGRES.md) ; ne jamais exposer les résultats contenant MMSI/positions.

## Étapes envisagées, **sur validation distincte de l'utilisateur**

1. **Terminée** : cadrage, dépôt privé et configuration locale.
2. **Implémentée, avec pilote NOAA local** : collecte asynchrone à reprise vers `data/raw/` JSONL ; tests de panne synthétiques. Capture AISStream réelle bloquée faute de clé.
3. **Implémentée, pilote NOAA transformé localement** : Parquet par source/date et agrégats candidats NOAA-only ; mois incomplet supprimé, tests synthétiques.
4. **Implémentée et testée localement** : PostgreSQL, chargement idempotent, cinq analyses SQL, `EXPLAIN ANALYZE` avant/après index mesuré.
5. Dataset Hub **d'abord privé**, contrôle bloquant des données, conformité NOAA et validation explicite avant toute ouverture publique.
6. Démonstration reproductible, mesures réelles et bilan ; tag final uniquement après validation.

Les étapes 5 et 6 ne sont **pas** faites. Aucune capture AISStream réelle ni URL Hub n'est disponible. Les critères de publication restent à confirmer dans `docs/DATA_POLICY.md` ; la journée NOAA pilote, seule, ne peut pas constituer un agrégat public.

## Références

- [Marine Cadastre AIS](https://marinecadastre.gov/ais/) ; [FAQ et conditions d'usage NOAA](https://coast.noaa.gov/data/marinecadastre/ais/faq.pdf) ; [index du produit NOAA 2024](https://noaaocm.blob.core.windows.net/ais/csv2/csv2024/index.html).
- [AISStream : protocole, limites, accès](https://aisstream.io/documentation).
- [Dataset Cards Hugging Face](https://huggingface.co/docs/hub/datasets-cards).

Consulter la politique détaillée **avant** tout téléchargement ou téléversement ; aucune licence de données n'est présumée.

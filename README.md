# Pipeline AIS cargos — étapes 1 et 2 : cadrage et ingestion

**Statut : ingestion NOAA/AISStream implémentée et testée, transformation/SQL/publication non implémentées.** Le pilote NOAA a été collecté localement sans publication. AISStream n'a pas été contacté faute de clé. Le dépôt reste privé. Voir [provenance, limites et règles de diffusion](docs/DATA_POLICY.md).

## Architecture prévue (non opérationnelle)

```text
NOAA (archive historique CSV.zst) ──┐
                                    ├─> data/raw/<source>/ JSONL ─> data/processed/<source>/ Parquet ─> PostgreSQL local
AISStream (WebSocket direct) ───────┘                       └─> agrégats NOAA-only contrôlés ─> Hub (privé jusqu'à validation)
```

Les sources conservent des provenance et espaces de stockage séparés. AISStream reste exclusivement local et n'alimente **jamais** le jeu public. NOAA ne couvre pas les traversées mondiales ; le code AIS 70–79 indique « cargo », pas spécifiquement « porte-conteneurs ».

## Préparer l'environnement et collecter **localement**

- Python **3.11 à 3.14**, gestionnaire d'environnement (`venv`/`pip` ou `uv`). Installer les dépendances : `python -m venv .venv && .venv/bin/python -m pip install -e '.[test]'` (installer pip au besoin), puis `.venv/bin/python -m pytest -q`. Alternative : `uv venv .venv && uv pip install --python .venv/bin/python 'aiohttp>=3.10,<4' 'zstandard>=0.23,<1' 'pytest>=8,<10'` pour les seules dépendances d'ingestion/tests.
- Vérifier l'espace disque avant l'archive nationale : `df -h .` ; l'ingestion refuse moins de **3 Go libres** et des archives compressées de plus de **1 Go**. Exécuter `.venv/bin/python -m scripts.ingest.noaa --date 2024-01-14` depuis la racine du dépôt. Source : archive NOAA quotidienne officielle CSV.zst nationale ; filtrage **à la lecture** du rectangle NY/NJ, sans chargement complet en RAM. Sortie `data/raw/noaa/2024-01-14-ny-nj.jsonl` et manifeste local. Une nouvelle exécution vérifie taille et SHA-256 avant de réutiliser le lot ; en cas d'interruption, le lot incomplet est reconstruit atomiquement. Téléchargements multi-jours internes limités à trois connexions ; HTTP 429/5xx et délais réessayés avec backoff. La taille max du JSONL est **2 Go**.
- AISStream **seulement si une clé personnelle est fournie** dans l'environnement : `AISSTREAM_API_KEY=... .venv/bin/python -m scripts.ingest.aisstream --duration 900 --session-id pilote-1` (ne pas écrire la valeur de la clé dans le terminal partagé, l'historique shell ou Git ; charger depuis un gestionnaire de secrets). Même identifiant pour reprendre une session interrompue, dans la limite des **15 minutes calendaires initiales** ; sinon nouvel identifiant. Une capture crée `data/raw/aisstream/<id>.jsonl` et un manifeste local ; les messages de position sans classification cargo statique préalable ne sont pas retenus. Une reconnexion **ne rejoue pas** les observations perdues ; déduplication locale à fenêtre bornée. La clé n'est pas disponible ici : aucun appel WebSocket réel n'a eu lieu.
- Pour la future étape SQL : Docker avec Compose et PostgreSQL ; aucun conteneur ni schéma fourni. Copier `.env.example` en `.env` (ignoré par Git) ; `DATABASE_URL` et `HF_TOKEN` sont réservés à plus tard. Les données NOAA/AISStream, manifests, checkpoints et fichiers temporaires sont exclus de Git par `.gitignore`. Les dossiers `scripts/transform/`, `scripts/load/`, `scripts/publish/` et `sql/` attendent leurs étapes respectives.

**Pilote NOAA réalisé localement** : archive compressée **151 376 897 octets**, **5 742 930 lignes** lues, **359 494** observations dans le rectangle, **0** ligne invalide, JSONL **193 327 768 octets** (manifeste local ignoré par Git). Ce sont des observations de tous types, **pas encore un compte de cargos** : le filtrage métier est réservé à la transformation. Aucun fichier de ce lot ne doit être ajouté au dépôt.

## Étapes envisagées, **sur validation distincte de l'utilisateur**

1. **Terminée** : cadrage, dépôt privé et configuration locale.
2. **Implémentée, avec pilote NOAA local** : collecte asynchrone à reprise vers `data/raw/` JSONL ; tests de panne synthétiques. Capture AISStream réelle bloquée faute de clé.
3. Normalisation Parquet et agrégats NOAA-only à confidentialité contrôlée ; tests synthétiques.
4. PostgreSQL local, chargement idempotent, cinq analyses SQL et `EXPLAIN ANALYZE` mesuré.
5. Dataset Hub **d'abord privé**, contrôle bloquant des données, conformité NOAA et validation explicite avant toute ouverture publique.
6. Démonstration reproductible, mesures réelles et bilan ; tag final uniquement après validation.

Les étapes 3 à 6 ne sont **pas** faites. Aucun résultat de transformation, de SQL, capture AISStream réelle ou URL Hub n'est disponible. Les critères de publication restent à confirmer dans `docs/DATA_POLICY.md` ; la journée NOAA pilote, seule, ne peut pas constituer un agrégat public.

## Références

- [Marine Cadastre AIS](https://marinecadastre.gov/ais/) ; [FAQ et conditions d'usage NOAA](https://coast.noaa.gov/data/marinecadastre/ais/faq.pdf) ; [index du produit NOAA 2024](https://noaaocm.blob.core.windows.net/ais/csv2/csv2024/index.html).
- [AISStream : protocole, limites, accès](https://aisstream.io/documentation).
- [Dataset Cards Hugging Face](https://huggingface.co/docs/hub/datasets-cards).

Consulter la politique détaillée **avant** tout téléchargement ou téléversement ; aucune licence de données n'est présumée.

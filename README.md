# Pipeline AIS cargos — étape 1 : cadrage et squelette

**Statut : seul le cadrage est réalisé.** Les scripts d'ingestion, transformation, chargement et publication, les tests et la base ne sont **pas encore implémentés**. Le dépôt est privé. Aucun jeu de données n'est collecté ni publié ici. Les choix pilotes sont provisoires : voir [provenance, limites et règles de diffusion](docs/DATA_POLICY.md).

## Architecture prévue (non opérationnelle)

```text
NOAA (archive historique CSV.zst) ──┐
                                    ├─> data/raw/<source>/ JSONL ─> data/processed/<source>/ Parquet ─> PostgreSQL local
AISStream (WebSocket direct) ───────┘                       └─> agrégats NOAA-only contrôlés ─> Hub (privé jusqu'à validation)
```

Les sources conservent des provenance et espaces de stockage séparés. AISStream reste exclusivement local et n'alimente **jamais** le jeu public. NOAA ne couvre pas les traversées mondiales ; le code AIS 70–79 indique « cargo », pas spécifiquement « porte-conteneurs ».

## Préparer un environnement (sans lancer de collecte)

- Python **3.11 à 3.14**, gestionnaire d'environnement Python (`venv`/`pip` ou `uv`). Dépendances prévues dans `pyproject.toml` ; par exemple `python -m venv .venv && .venv/bin/python -m pip install -e '.[test]'` (installer pip au besoin). Aucun test applicatif n'existe à ce stade.
- Pour la future étape SQL : Docker avec Compose et PostgreSQL ; aucun conteneur ni schéma n'est encore fourni.
- Copier `.env.example` en `.env` (ignoré par Git). La future collecte AISStream exigera `AISSTREAM_API_KEY` personnelle ; `DATABASE_URL` sera nécessaire au chargement PostgreSQL, `HF_TOKEN` au Hub. Ne jamais partager les valeurs ni les placer dans Git. La clé AISStream n'est pas disponible, donc l'accès authentifié n'a pas été vérifié.
- `data/raw/` et `data/processed/` ne contiennent que des marqueurs ; leurs futurs contenus (y compris checkpoints/manifests) sont exclus de Git. Les dossiers `scripts/ingest/`, `scripts/transform/`, `scripts/load/`, `scripts/publish/`, `sql/`, `tests/` attendent leurs étapes respectives.

## Étapes envisagées, **sur validation distincte de l'utilisateur**

1. **Terminée** : cadrage, dépôt privé et configuration locale.
2. Collecte asynchrone à reprise vers `data/raw/` JSONL ; tests de panne et échantillon pilote.
3. Normalisation Parquet et agrégats NOAA-only à confidentialité contrôlée ; tests synthétiques.
4. PostgreSQL local, chargement idempotent, cinq analyses SQL et `EXPLAIN ANALYZE` mesuré.
5. Dataset Hub **d'abord privé**, contrôle bloquant des données, conformité NOAA et validation explicite avant toute ouverture publique.
6. Démonstration reproductible, mesures réelles et bilan ; tag final uniquement après validation.

Ne pas exécuter ni considérer ces étapes comme faites. Aucun résultat, mesure de performance, capture AIS ou URL Hub n'est disponible. Les critères de publication et le périmètre pilote (jour 2024-01-14, côte NY/NJ, 15 minutes AISStream) sont documentés comme **propositions à confirmer** dans `docs/DATA_POLICY.md`.

## Références

- [Marine Cadastre AIS](https://marinecadastre.gov/ais/) ; [FAQ et conditions d'usage NOAA](https://coast.noaa.gov/data/marinecadastre/ais/faq.pdf) ; [index du produit NOAA 2024](https://noaaocm.blob.core.windows.net/ais/csv2/csv2024/index.html).
- [AISStream : protocole, limites, accès](https://aisstream.io/documentation).
- [Dataset Cards Hugging Face](https://huggingface.co/docs/hub/datasets-cards).

Consulter la politique détaillée **avant** tout téléchargement ou téléversement ; aucune licence de données n'est présumée.

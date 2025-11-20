MigrationPanda
==============

Ce projet contient deux pipelines de transformation des ventes :

* `src/pandas/pipeline_pandas.py` — version Pandas.
* `src/pyspark/pipeline_pyspark.py` — version PySpark.

Les deux pipelines lisent les mêmes fichiers depuis `settings.yaml`, filtrent/nettoient les commandes, calculent les agrégations quotidiennes, et écrivent le résultat dans SQLite + CSV.


Prérequis
---------

* Python ≥ 3.12.
* `uv` (recommandé pour gérer l’environnement virtuel).
* Dépendances (installées via `uv sync` ou `pip install -r requirements` équivalent) :
  * pandas
  * pyspark
  * jupyter (facultatif, utilisé pour les notebooks)
* Pour la suite de tests : `pytest` (facultatif), sinon exécuter directement les scripts.


Installation rapide
-------------------

```bash
# synchronise les dépendances déclarées dans pyproject.toml
uv sync

# active l'environnement puis lance la CLI si besoin
source .venv/bin/activate
```


Exécution des pipelines
-----------------------

1. Préparer les fichiers sources dans `data/march-input` (ou modifier `settings.yaml` pour pointer vers un autre dossier). Les fichiers attendus :
   * `customers.csv`
   * `refunds.csv`
   * `orders_2025-03-XX.json` pour chaque jour disponible.

2. Ajuster `settings.yaml` si nécessaire (dossiers, encodage CSV, chemin SQLite…).

3. Lancer la version Pandas :

```bash
uv run python src/pandas/pipeline_pandas.py
```

4. Lancer la version PySpark :

```bash
uv run python src/pyspark/pipeline_pyspark.py
```

Les sorties sont écrites dans le dossier `output_dir` défini dans `settings.yaml` (CSV `daily_summary_*` + base SQLite contenant `orders_clean` et `daily_city_sales`).


Tests d’équivalence
-------------------

Un test vérifie que les deux pipelines produisent les mêmes résultats sur un jeu de données synthétique :

```bash
# mode script autonome avec logs détaillés
uv run python tests/test_pipeline_equivalence.py

# (optionnel) exécution via pytest si installé
uv run python -m pytest tests/test_full_suite.py
```

Le test signale les éventuelles différences (colonnes, lignes, types, valeurs) entre les sorties Pandas et PySpark. Les écarts string/object sont tolérés mais loggués.


Pre-commit (optionnel)
----------------------

Un fichier `.pre-commit-config.yaml` configure Black, isort, Ruff et quelques hooks génériques. Pour l’activer :

```bash
uv pip install pre-commit  # nécessite un accès réseau
pre-commit install
pre-commit run --all-files
```

Cette étape est facultative mais recommandée pour garder un formatage cohérent.

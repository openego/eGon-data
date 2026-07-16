# egon-data metadata

OEMetadata v2 metadata for all tables published by the eGon-data
pipeline, processed with [OMI](https://github.com/OpenEnergyPlatform/omi).

The version-controlled split-YAML store under `dataset_metadata/` is the
authoritative source of metadata content (one OEMetadata dataset per DAG
task group, one resource YAML per published table, task-group → dataset-id
mapping in `dataset_metadata/dataset_ids.yaml`).

Contents of this package:

- `settings.py` — store paths and the OEMetadata version.
- `inventory.py` — static enumeration of declared
  `DatasetSources`/`DatasetTargets` per pipeline class (AST-based, no
  airflow/DB imports).
- `cli.py` — the `egon-data metadata status|init|export` developer
  commands.
- `__init__.py` — the `Json_Metadata` pipeline task that assembles,
  validates and publishes metadata at the end of every run.
- `results/` — legacy OEMetadata v1.5 JSON documents kept as the
  recovery source for the bulk migration.
- `script/` — one-off migration scripts (v1.5 → v2 conversion, JSON →
  split-YAML).

Documentation:

- Developer how-to (add a dataset, edit metadata, release runs):
  `docs/metadata_howto.rst`
- Technical reference (architecture, design decisions, run-time flow):
  `docs/metadata.rst`

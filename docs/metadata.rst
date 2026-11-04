***************
Metadata system
***************

.. note::

   If you just want to get your dataset documented — or a release is
   coming up and you need to know what to do — skip the architecture and
   go straight to the hands-on guide: :doc:`metadata_howto`.

eGon-data attaches `OEMetadata v2
<https://github.com/OpenEnergyPlatform/oemetadata>`_ metadata to every
database table it publishes. The metadata content is maintained as
version-controlled YAML files inside this repository and is processed by
`OMI <https://github.com/OpenEnergyPlatform/omi>`_ (Open Metadata
Integration); eGon-data itself contains only thin orchestration around
OMI's generic functionality.

Design principles
=================

Git is the metadata store
   The split-YAML files under
   ``src/egon/data/metadata/dataset_metadata/`` are the single
   authoritative source of metadata content. History and versioning are
   plain git history and release tags. Everything else — database table
   comments, exported JSON files — is a generated snapshot. (A dedicated
   JSONB metadata table inside the database is planned but deliberately
   deferred.)

``DatasetSources`` / ``DatasetTargets`` is the registry
   There is no separate list of "tables that need metadata". A table
   needs metadata the moment some pipeline ``Dataset`` class declares it
   in its ``DatasetTargets``; documented means a matching resource YAML
   exists. Coverage is computed from exactly this. Declared
   ``DatasetSources`` drive provenance (where did the input data come
   from).

Structural truth from the database, descriptive truth from humans
   Column names, types and primary keys are inspected from the live
   database; descriptions, titles and licenses are written by people in
   the YAML files. The two are merged non-destructively: regeneration
   never overwrites human-written text.

One table, one resource
   A table written by several task groups (the eTraGo tables are written
   by up to twelve) is documented exactly once, in the dataset of its
   *owning* task group, named under ``table_owners`` in
   ``dataset_ids.yaml``. The rule is "the group that creates the table",
   with one exception: the ``grid.egon_etrago_*`` tables are created in
   ``setup`` but published as eTraGo input. A shared table without an
   owner is an error in ``status`` and in CI.

Drift never breaks the pipeline
   If a table gains a column that the YAML does not describe, the run
   merges the column with a ``TODO`` description placeholder and logs a
   warning — the published metadata stays structurally truthful, and the
   gap surfaces in ``egon-data metadata status`` until someone fills it
   in git.

Terminology
===========

An eGon-data ``Dataset`` (a pipeline class) is **not** an OEMetadata
dataset. One OEMetadata dataset spans one DAG *task group* (e.g.
``heat_demand``, ``gas_grid``) and typically contains the tables of
several pipeline classes. OEMetadata datasets serve publication,
findability and reproducibility; pipeline classes serve execution. The
mapping between the two lives in one file:
``dataset_metadata/dataset_ids.yaml`` (task group → dataset id, ids
follow ``egon_<task_group>``; plus ``table_owners`` for shared tables).
Dataset classes that are not wired into the DAG produce nothing and are
not expected to carry metadata.

The metadata store
==================

Layout (`OMI's split-files convention
<https://github.com/OpenEnergyPlatform/omi>`_)::

   src/egon/data/metadata/dataset_metadata/
   ├── dataset_ids.yaml               # task group -> dataset id, table owners
   ├── datasets/
   │   ├── egon_heat_demand.dataset.yaml    # dataset-level fields
   │   ├── egon_heat_demand.template.yaml   # shared per-resource content
   │   └── ...
   └── resources/
       ├── egon_heat_demand/
       │   ├── demand_egon_etrago_heat_cts.resource.yaml
       │   └── ...                    # one file per published table
       └── ...

- ``<id>.dataset.yaml`` holds the dataset-level name/title/description.
- ``<id>.template.yaml`` holds content shared by all of the dataset's
  resources (project context, funding, default licenses, keywords). At
  assembly time OMI applies the template to each resource; the resource
  file only needs what is specific to its table.
- ``resources/<id>/<schema>_<table>.resource.yaml`` is the per-table
  file: title, description, ``schema.fields`` (column descriptions),
  ``sources`` (provenance), licenses, spatial/temporal extent. The
  ``name`` field inside is the real ``schema.table`` identifier; the
  filename replaces the dot with an underscore.
- Field ``type`` values are the database's own type names (``bigint``,
  ``text``, ``double precision``, ``geometry(Point,3035)``), as on the
  OEP — ``init`` and the run task take them from PostgreSQL directly.
- ``licenses`` in a resource *replaces* the template's default; use it
  whenever a table's data is not under the eGon default license (e.g.
  OSM-derived tables: ODbL with "© OpenStreetMap contributors").
  ``keywords``, ``topics`` and ``languages`` are *added* to the
  template's.

Components
==========

``egon.data.metadata.settings``
   Store paths (``OEM_BASE_DIR``, ``OEM_OUT_DIR``) and the OEMetadata
   version identifier.

``egon.data.metadata.inventory``
   Static enumeration of every pipeline class's declared
   ``DatasetSources``/``DatasetTargets`` and its DAG task group. Works
   by AST-parsing the source tree and ``pipeline.py`` — no airflow or
   database imports — so it runs in CI and on any checkout. This module
   answers "which tables must be documented" and "which dataset id does
   a table belong to" (including the owner of a shared table).

``egon.data.metadata.cli``
   The ``egon-data metadata status|init|export`` developer commands
   (see :doc:`metadata_howto` for usage). Registered as a subcommand
   group that bypasses the workflow bootstrap — no Docker, no airflow.

``egon.data.metadata`` (``Json_Metadata``)
   The pipeline task that runs last in the DAG; see next section.

OMI capabilities used
   Assembly of split YAML into full documents
   (``omi.creation.assembler``), the metadata builder with
   non-destructive DB-schema merge (``omi.creation.builder``), DB table
   inspection (``omi.inspection``), resource/dataset scaffolding and
   merge-update (``omi.creation.init``), provenance source helpers
   (``omi.creation.sources``), coverage classification
   (``omi.creation.coverage``), and v1.5→v2 conversion
   (``omi.conversion``) for migrating legacy metadata.

Run-time flow
=============

The ``Json_Metadata`` task executes after all other tasks::

   for each dataset id in the store (datasets/*.dataset.yaml):
       assemble document (dataset yaml + template + resource yamls)
       inject publicationDate + automated-run contributor
       for each resource:
           table present in this database?
               no  -> INFO, skip (partial dev runs stay valid)
               yes -> inspect DB schema, merge non-destructively
                      new DB column      -> merged with TODO description,
                                            WARNING
                      column type change -> structural truth wins, WARNING
                      column gone from DB-> kept in YAML, WARNING
       validate against the OEMetadata schema (hard error)
       for each present table:
           write single-resource JSON as the table's COMMENT
           write workdir/oemetadata/<schema>.<table>.json

Consequences:

- Every run of any database produces metadata describing exactly the
  tables that database contains.
- The one-JSON-file-per-table requirement is satisfied on every run as
  a by-product; ``egon-data metadata export`` regenerates the same files
  offline from the YAML store alone.
- Validation failures are real errors (the YAML no longer conforms to
  the OEMetadata schema); drift is never a failure.

.. _metadata-coverage-states:

Coverage states
===============

``egon-data metadata status`` classifies every declared target table:

missing
   No resource YAML exists — the hard, CI-blocking tier.
skeleton
   A YAML exists, but required human content is still blank or carries a
   ``TODO``/scaffolding placeholder — visible warning tier.
complete
   Fully documented.
orphan
   A resource YAML exists for a table no pipeline class declares as a
   target — either dead metadata or (more likely) a missing
   ``DatasetTargets`` declaration on the producer.

ambiguous
   A table is written by several task groups and ``table_owners`` does
   not say which one documents it — CI-blocking like ``missing``.

Additionally, consumed *source* tables that are produced inside eGon-data
but have no metadata of their own are reported as provenance warnings.

Status and planned work
=======================

Implemented
   Split store with per-task-group datasets, ``dataset_ids.yaml`` with
   table owners, static inventory, the three CLI commands, the
   multi-dataset run task with drift-tolerant schema merge and per-table
   JSON export, and the CI gate (``tests/test_metadata_store.py``, runs
   in the normal test job without a database). Every table the pipeline
   produces has metadata; how much of it is still ``TODO`` is shown by
   ``egon-data metadata status``. How the store was filled from the
   legacy metadata is described in :doc:`metadata_migration`.

Requires
   OMI ≥ 1.3.0 (creation helpers, database-native column types).

Planned
   A JSONB metadata table (``metadata.oemetadata``) replacing the
   full-JSON table comments with a one-line pointer comment; minting the
   ``@id`` IRI at publication time; a push interface beyond the OEP; one
   store-wide template in OMI instead of one identical copy per dataset.

.. toctree::
   :maxdepth: 1
   :hidden:

   metadata_howto
   metadata_migration

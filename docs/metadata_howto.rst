********************************
Metadata how-to (for developers)
********************************

You do not need to know anything about metadata standards to work on
this pipeline. This page covers the three situations where metadata
concerns you, with the exact commands. Everything else — assembly,
validation, standards compliance, publishing — is automated.

The one-line summary: **declare your tables on your Dataset class, run
two commands, fill in the TODOs in one YAML file, commit it with your
code.**

Cheat sheet
===========

===============================================  ===================================================
I want to …                                      Command
===============================================  ===================================================
see what's missing for my task group             ``egon-data metadata status --scope heat_demand``
create/update the YAML skeletons for my scope    ``egon-data metadata init --scope heat_demand``
check the whole store (what CI checks)           ``egon-data metadata status``
regenerate the per-table JSON files, no DB       ``egon-data metadata export``
===============================================  ===================================================

``--scope`` accepts a task group (``heat_demand``), an OEMetadata
dataset id (``egon_heat_demand``), or a single Dataset class name
(``DistrictHeatingAreas``). Without it, all datasets are processed.
None of these commands start Docker or airflow, and none of them need a
database (with one they do more — see below).

Use case 1: I added a new dataset
=================================

**Step 1 — declare your inputs and outputs on the class.** This is the
only registration there is; if it's not declared here, the metadata
system doesn't know your table exists:

.. code-block:: python

   from egon.data.datasets import (
       Dataset, DatasetSources, DatasetTargets,
   )

   class MyNewDataset(Dataset):
       sources = DatasetSources(
           tables={
               "mv_grid_districts": "grid.egon_mv_grid_district",
           },
           urls={
               "mastr": "https://download.marktstammdatenregister.de/...",
           },
       )
       targets = DatasetTargets(
           tables={
               "my_result": "supply.egon_my_result_table",
           },
       )

**Step 2 (recommended) — run your dataset locally** so the table exists
in your local database. Not required: without a database the generator
still writes a skeleton, only the column list is left for later.

**Step 3 — generate the skeleton:**

.. code-block:: bash

   egon-data metadata init --scope MyNewDataset

This creates
``…/dataset_metadata/resources/<dataset id>/supply_egon_my_result_table.resource.yaml``
with:

- the column list and types inspected from your local database (each
  column carrying ``description: TODO: Add description``),
- one provenance entry per declared source — internal tables become
  cross-references automatically, URLs/files become external source
  stubs,
- blank fields for everything only you can know.

**Step 4 — fill in the human part.** Open the YAML and replace the
blanks and ``TODO``\ s. What actually matters:

- ``title`` — one line, human readable
- ``description`` — a short paragraph: what is in this table, how was
  it derived
- ``schema.fields[*].description`` — one line per column (plus ``unit``
  where applicable)
- the external source stubs — add title/author/license of the input
  data where the generator could only fill in the URL

You can ignore the rest of the file; shared content (project context,
default licenses, keywords) is inherited from the dataset template.

**Step 5 — verify and commit:**

.. code-block:: bash

   egon-data metadata status --scope MyNewDataset

When your table shows ``complete``, commit the YAML **together with your
code change**. Done — publication happens automatically from here on.

.. note::

   If your task group is new (not yet in
   ``dataset_metadata/dataset_ids.yaml``), add one line there — task
   group name on the left, ``egon_<task_group>`` on the right.

Use case 2: I want to edit or update existing metadata
======================================================

Metadata content lives in one YAML file per table::

   src/egon/data/metadata/dataset_metadata/resources/<dataset id>/<schema>_<table>.resource.yaml

Edit it like any other file and commit. That's the whole workflow —
git is the metadata store, so a metadata fix is a normal PR.

Two helpers:

- **My table's columns changed** (new column, changed type):

  .. code-block:: bash

     egon-data metadata init --scope <your scope>

  updates the ``schema.fields`` list from your local database
  *non-destructively*: new columns are added with a ``TODO``
  description, your existing descriptions are never touched, and
  columns that vanished from the database are reported, not deleted.
  "Create" and "refine" are the same command run twice.

- **Did I break anything?**

  .. code-block:: bash

     egon-data metadata status --scope <your scope>   # completeness
     egon-data metadata export --scope <your scope>   # full assembly + JSON

  If ``export`` runs through, your YAML still assembles into a valid
  document.

Use case 3: a full run happened and a release is coming up
==========================================================

During the run you did nothing: the ``JsonMetadata`` task runs last and
automatically attaches up-to-date metadata (as a table comment) to every
published table in that database, stamped with the run's publication
date, and writes one ``<schema>.<table>.json`` file per table to
``workdir/oemetadata/``.

What to do around the release:

**1 — check the run log for drift warnings.** Lines like::

   [supply.egon_my_result_table] Undocumented columns in DB (merged with
   TODO description): ['new_column']

mean the data changed but the descriptions didn't. The run is fine —
the published metadata contains the new column with a ``TODO``
placeholder — but fix it in git before the release:
``egon-data metadata init --scope …``, fill the ``TODO``, commit.

**2 — run the full coverage check:**

.. code-block:: bash

   egon-data metadata status          # human-readable
   egon-data metadata status --strict # exits non-zero on missing (CI)

For a release, ``missing`` should be empty for all published tables and
ideally nothing relevant is left in ``skeleton``.

**3 — hand over the metadata artifacts.** The per-table JSON files in
``workdir/oemetadata/`` of the release run are the publishable metadata
documents, consistent with the release database. (They can be
regenerated anytime from the repo with ``egon-data metadata export`` —
without the run-specific publication date and DB schema merge.)

**4 — tag the release.** The git tag on the repository *is* the
metadata version: anyone can reconstruct exactly the metadata belonging
to a data release from the tagged ``dataset_metadata/`` tree.

Troubleshooting
===============

``[missing] my.table (produced by MyClass)`` in status
   No resource YAML exists yet → use case 1, step 3.

``[orphan] some.table``
   A YAML exists but no class declares the table in ``DatasetTargets``
   → either the producing class is missing the declaration (add it), or
   the table is genuinely gone (delete the YAML).

``[source] x.y is consumed … but has no metadata yet``
   Your dataset reads an internal table that isn't documented. Nothing
   you must fix in *your* dataset — it flags the gap on the producer's
   side.

``No local database reachable``
   Fine. ``status`` and ``export`` are fully functional without one;
   ``init`` writes skeletons without the column lists and fills them in
   the next time you run it with a database present.

Validation error during a pipeline run
   The assembled document no longer conforms to the OEMetadata schema —
   usually a malformed edit in a resource YAML. Run
   ``egon-data metadata export --scope <dataset id>`` locally to
   reproduce, fix the YAML, commit.

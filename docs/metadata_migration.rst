******************************************
Metadata migration to OEMetadata v2 (2026)
******************************************

This page records how the metadata store described in :doc:`metadata`
was filled from the legacy metadata, what the migration changed, and
what is left to do. It is a one-time record; for day-to-day work see
:doc:`metadata_howto`.

Starting point
==============

- Metadata was written as OEMetadata **v1.4** JSON by hand-written
  ``metadata()`` / ``add_metadata()`` functions inside the dataset code
  (about 3,700 lines in 23 files) and stored as the table comment of
  each table at the end of its dataset. Nothing validated it.
- The new YAML store held **20** resources, converted from the v1.5
  JSON files under ``metadata/results/``.
- The pipeline produces **158** tables (counted from the
  ``DatasetTargets`` declarations of every Dataset class in the DAG).

Sources of the migrated metadata
================================

Reference run
   The full run ``regon_dev_02-10`` on the project server. Of its 218
   tables, **92** carry a metadata comment, all OEMetadata v1.4.0 or
   v1.4.1. No system, PostGIS or ``public`` table carries one. The
   Zenodo-published database holds only 19 comments, all also present in
   the reference run.

Precedence
   Curated store YAML > table comment of the reference run > v1.5 JSON in
   ``metadata/results/``. The 20 existing resources were not overwritten.

Conversion
   OMI converts from v1.5.2 upwards, so each comment's
   ``metadataVersion`` was set to ``OEP-1.5.2`` first; all 92 comments
   convert. Only technical defects were repaired:

   - ``NaN`` values and the ``WILL_BE_SET_AT_PUBLICATION`` placeholder
     ``@id`` became ``null``;
   - JSON that was encoded twice, strings quoted twice, and a source
     license nested inside its own ``name``;
   - the eTraGo tables' fields, generated from PyPSA's component tables,
     named the column ``attribute`` instead of ``name``;
   - literal ``\uXXXX`` and ``\n`` sequences in the four dataset templates
     and ten resources.

   Content gaps — a ``TODO`` description, empty field descriptions —
   were kept and are visible in ``egon-data metadata status``.

Tables without any legacy metadata
   Got a skeleton from ``egon-data metadata init`` against the reference
   run: columns, database types, nullability, primary key, provenance
   stubs from the declared sources, ``TODO`` for every description.

Structure
   Every resource whose table exists in the reference run (147 of 158)
   was then aligned with it: field types are PostgreSQL's own type names,
   undocumented columns were added with a ``TODO`` description, and
   fields describing columns that do not exist were removed (list below).

Result
======

Every table the pipeline produces now has metadata, every dataset
assembles to valid OEMetadata v2, and the CI test job keeps it that way.

==============================  ========  ========  ========  ========  ==========  ==========
Dataset                         Tables    before    legacy    new       complete    skeleton
==============================  ========  ========  ========  ========  ==========  ==========
egon_input_data                 47        3         17        27        17          30
egon_electricity_demand         20        5         0         15        2           18
egon_heat_demand                17        11        3         3         12          5
egon_mobility_demand            14        0         0         14        1           13
egon_etrago_input               13        0         13        0         0           13
egon_electricity_supply         13        0         11        2         2           11
egon_electricity_grid           8         1         0         7         1           7
egon_setup                      6         0         4         2         0           6
egon_electricity_flexibility    4         0         4         0         0           4
egon_gas_grid                   4         0         1         3         1           3
egon_heat_supply                3         0         2         1         2           1
egon_combined_heat_and_power    2         0         1         1         1           1
egon_scenario_capacities        2         0         1         1         1           1
egon_mapping                    2         0         0         2         1           1
egon_gas_supply                 1         0         0         1         0           1
egon_mobility_supply            1         0         1         0         0           1
egon_load_areas                 1         0         0         1         0           1
**total**                       158       20        58        80        41          117
==============================  ========  ========  ========  ========  ==========  ==========

*before*: resource existed before the migration; *legacy*: converted
from the reference run's table comment; *new*: created as a skeleton —
seven of them were completed by hand (ETHOS.BUILDA, rail and public
transport, Zensus). ``complete``/``skeleton`` as defined in
:ref:`the coverage states <metadata-coverage-states>`.

What improved
=============

- **One standard, validated.** All metadata is OEMetadata v2 and is
  validated in CI and on every run; before, nothing checked it.
- **Reviewable.** Metadata is one YAML file per table in git, changed in
  normal pull requests next to the code it describes; ``dataset_metadata/``
  replaced 3,700 lines of Python dictionaries.
- **Complete by construction.** The tables that need metadata are derived
  from ``DatasetTargets``; a new table without metadata fails CI.
- **Structurally true.** Columns, types and keys come from the database,
  so the documentation cannot silently describe columns that no longer
  exist — the migration found and removed 18 such fields.
- **One table, one document.** Tables written by several task groups (the
  eTraGo tables by up to twelve) are documented once, by their owner.

Defects found and fixed on the way
==================================

In eGon-data
   - The coverage inventory did not see the seven OpenStreetMap raw
     tables (declared as a list) and counted two tables of a class that is
     not in the DAG.
   - ``metadata status --scope`` reported a dataset's other tables as
     orphans and shared tables as missing.
   - The run task stamped the publication date and its contributor on the
     dataset level, where OEMetadata v2 does not look for them; published
     resources kept the date of their YAML.

In OMI (released in 1.3.0)
   - Database inspection reported frictionless types (``string``,
     ``integer``) instead of the database's own types, turning every
     geometry, ``hstore`` and ``jsonb`` column into ``string``; merge-update
     then overwrote curated types with them.
   - The v1.5 conversion split a string ``primaryKey`` into characters.
   - The conversion contributor used the key ``role`` (v2 has ``roles``)
     and a fixed date of 2021.
   - OMI required ``pandas>=2.2.2``, which cannot be installed next to
     eGon-data's ``pandas==2.0.3``.

Fields removed because the column does not exist
================================================

Where only the name changed, the description moved to the new column.

===============================================================  =================================================================
Table                                                            Change
===============================================================  =================================================================
``supply.egon_power_plants``                                     ``geometry`` → ``geom``
``openstreetmap.osm_buildings_filtered``                         ``amenitiy`` → ``amenity``
``grid.egon_etrago_transformer``, ``…_timeseries``               ``transformer_id`` → ``trafo_id``
``grid.egon_etrago_carrier``                                     ``carrier_id`` → ``name``; ``max_growth``, ``scn_name`` removed
``grid.egon_etrago_bus``                                         ``unit`` removed
``grid.egon_etrago_bus_timeseries``                              ``temp_id`` removed
``grid.egon_etrago_link``                                        ``ramp_limit_up``, ``ramp_limit_down`` removed
``grid.egon_etrago_storage``                                     ``cyclic_state_of_charge_per_period``,
                                                                 ``state_of_charge_initial_per_period`` removed
``grid.egon_etrago_store``                                       ``e_cyclic_per_period``, ``e_initial_per_period`` removed
``grid.egon_mv_grid_district``                                   ``building_id`` removed (copied from another table)
``boundaries.egon_map_zensus_climate_zones``                     ``w_id`` removed; the table has ``climate_zone``
``demand.egon_household_electricity_profile_in_census_cell``     ``factor_2050`` removed; the table has ``factor_2024/2037/2045``
===============================================================  =================================================================

What is left to do
==================

``egon-data metadata status`` lists every ``skeleton`` with its reasons;
``--scope <task group>`` narrows it to one team's tables. The biggest
blocks:

- **Descriptions of the 80 new skeletons** — mainly ``input_data`` (OSM,
  VG250 and demandregio tables), ``electricity_demand`` (industrial load
  curves) and ``mobility_demand``.
- **The eTraGo tables** (``egon_etrago_input``, ``egon_setup``): the field
  descriptions are complete from PyPSA, but the legacy metadata never had
  a table description, and the columns eGon-data adds (``scn_name``,
  ``geom``, ``topo``, …) need one.
- **Field descriptions of the power plant tables** (``supply.egon_power_plants_*``),
  which the legacy metadata left empty.
- **Eleven tables are not in the reference run** (e.g. the old
  ``emobility.egon_ev_*`` tables) and are stubs without fields until
  ``init`` runs against a database that has them.
- **Default license of the dataset templates.** It is the Zensus license
  (``dl-by-de/2.0``, "© Statistische Ämter des Bundes und der Länder"),
  hoisted from the first converted resources, and applies silently to
  every table without its own ``licenses`` block — while 62 of the 92
  legacy comments declare ODbL-1.0 "© eGon development team".
- **Source path of NEP 2037/2045, Version 2025** in
  ``supply.egon_scenario_capacities`` is still the placeholder ``folgt``.

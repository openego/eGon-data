"""
Filtered and preprocessed buildings, streets and amenities from OpenStreetMap
(OSM)

This dataset on buildings and amenities is required by several tasks in the
pipeline, such as the distribution of household demand profiles or PV home
systems to buildings. This data is enriched by population and apartments from
Zensus 2011. Those derived datasets and the data on streets will be used in the
DIstribution Network Generat0r :ref:`ding0
<https://github.com/openego/ding0>`_ e.g. to cluster loads and create low
voltage grids.

**Details and Steps**

* Extract buildings and filter using relevant tags, e.g. residential and
  commercial, see script `osm_buildings_filter.sql` for the full list of tags.
  Resulting tables:
  * All buildings: `openstreetmap.osm_buildings`
  * Filtered buildings: `openstreetmap.osm_buildings_filtered`
  * Residential buildings: `openstreetmap.osm_buildings_residential`
* Extract amenities and filter using relevant tags, e.g. shops and restaurants,
  see script `osm_amenities_shops_preprocessing.sql` for the full list of tags.
  Resulting table: `openstreetmap.osm_amenities_shops_filtered`
* Create a mapping table for building's osm IDs to the Zensus cells the
  building's centroid is located in.
  Resulting tables:
  * `boundaries.egon_map_zensus_buildings_filtered` (filtered)
  * `boundaries.egon_map_zensus_buildings_residential` (residential only)
* Enrich each building by number of apartments from Zensus table
  `society.egon_destatis_zensus_apartment_building_population_per_ha`
  by splitting up the cell's sum equally to the buildings. In some cases, a
  Zensus cell does not contain buildings but there's a building nearby which
  the no. of apartments is to be allocated to. To make sure apartments are
  allocated to at least one building, a radius of 77m is used to catch building
  geometries.
* Split filtered buildings into 3 datasets using the amenities' locations:
  temporary tables are created in script `osm_buildings_temp_tables.sql` the
  final tables in `osm_buildings_amentities_results.sql`.
  Resulting tables:
  * Buildings w/ amenities: `openstreetmap.osm_buildings_with_amenities`
  * Buildings w/o amenities: `openstreetmap.osm_buildings_without_amenities`
  * Amenities not allocated to buildings:
    `openstreetmap.osm_amenities_not_in_buildings`
* Extract streets (OSM ways) and filter using relevant tags, e.g.
  highway=secondary, see script `osm_ways_preprocessing.sql` for the full list
  of tags. Additionally, each way is split into its line segments and their
  lengths is retained.
  Resulting tables:
  * Filtered streets: `openstreetmap.osm_ways_preprocessed`
  * Filtered streets w/ segments: `openstreetmap.osm_ways_with_segments`

Notes
-----

This module docstring is rather a dataset documentation. Once, a decision
is made in ... the content of this module docstring needs to be moved to
docs attribute of the respective dataset class.
"""

import os

from egon.data import db
from egon.data.datasets import Dataset


def execute_sql_script(script):
    """Execute SQL script

    Parameters
    ----------
    script : str
        Filename of script
    """
    db.execute_sql_script(os.path.join(os.path.dirname(__file__), script))


def preprocessing():
    print("Extracting buildings, amenities and shops...")
    sql_scripts = [
        "osm_amenities_shops_preprocessing.sql",
        "osm_buildings_extract.sql",
    ]
    for script in sql_scripts:
        execute_sql_script(script)


def filter_buildings():
    print("Filter buildings...")
    execute_sql_script("osm_buildings_filter.sql")


def filter_buildings_residential():
    print("Filter residential buildings...")
    execute_sql_script("osm_buildings_filter_residential.sql")


def create_buildings_filtered_zensus_mapping():
    print("Create census mapping table for filtered buildings...")
    execute_sql_script("osm_buildings_filtered_zensus_mapping.sql")


def create_buildings_residential_zensus_mapping():
    print("Create census mapping table for residential buildings...")
    execute_sql_script("osm_buildings_residential_zensus_mapping.sql")


def create_buildings_temp_tables():
    print("Create temp tables for buildings...")
    execute_sql_script("osm_buildings_temp_tables.sql")


def extract_buildings_w_amenities():
    print("Extracting buildings with amenities...")
    execute_sql_script("osm_results_buildings_w_amenities.sql")


def extract_buildings_wo_amenities():
    print("Extracting buildings without amenities...")
    execute_sql_script("osm_results_buildings_wo_amenities.sql")


def extract_amenities():
    print("Extracting amenities...")
    execute_sql_script("osm_results_amenities.sql")


def extract_ways():
    print("Extracting ways...")
    execute_sql_script("osm_ways_preprocessing.sql")


def drop_temp_tables():
    print("Dropping temp tables...")
    execute_sql_script("drop_temp_tables.sql")


def add_metadata():
    pass


class OsmBuildingsStreets(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="OsmBuildingsStreets",
            version="0.0.5",
            dependencies=dependencies,
            tasks=(
                preprocessing,
                {filter_buildings, filter_buildings_residential},
                {
                    create_buildings_filtered_zensus_mapping,
                    create_buildings_residential_zensus_mapping,
                },
                create_buildings_temp_tables,
                {
                    extract_buildings_w_amenities,
                    extract_buildings_wo_amenities,
                    extract_amenities,
                    extract_ways,
                },
                drop_temp_tables,
                add_metadata,
            ),
        )

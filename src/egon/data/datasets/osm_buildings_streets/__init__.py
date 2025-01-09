"""
Filtering and preprocessing of buildings, streets and amenities from OpenStreetMap

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


def extend_buildings_residential():
    print(
        "Extend residential buildings by commercial/retail buildings in cells "
        "with census population but without buildings..."
    )
    execute_sql_script("osm_buildings_extend_residential.sql")


def create_buildings_filtered_zensus_mapping():
    print(
        "Create census mapping table for filtered buildings in populated areas..."
    )
    execute_sql_script("osm_buildings_filtered_zensus_mapping.sql")


def create_buildings_filtered_all_zensus_mapping():
    print("Create census mapping table for all filtered buildings...")
    execute_sql_script("osm_buildings_filtered_all_zensus_mapping.sql")


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


def extract_buildings_filtered_amenities():
    print("Extracting buildings filtered with and without amenities...")
    execute_sql_script("osm_buildings_filter_amenities.sql")


def extract_ways():
    print("Extracting ways...")
    execute_sql_script("osm_ways_preprocessing.sql")


def drop_temp_tables():
    print("Dropping temp tables...")
    execute_sql_script("drop_temp_tables.sql")


def add_metadata():
    pass


class OsmBuildingsStreets(Dataset):
    """
    Filter and preprocess buildings, streets and amenities from OpenStreetMap (OSM).

    This dataset on buildings and amenities is required by several tasks in the
    pipeline, such as the distribution of household demand profiles or PV home
    systems to buildings. This data is enriched by population and apartments from
    Zensus 2011. Those derived datasets and the data on streets will be used in the
    DIstribution Network Generat0r
    `ding0 <https://github.com/openego/ding0>`_ e.g. to cluster loads and create low
    voltage grids.

    *Dependencies*
      * :py:class:`OpenStreetMap <egon.data.datasets.osm.OpenStreetMap>`
      * :py:class:`ZensusMiscellaneous <egon.data.datasets.zensus.ZensusMiscellaneous>`

    *Resulting Tables*
      * openstreetmap.osm_buildings is created and filled (table has no associated python class)
      * openstreetmap.osm_buildings_filtered is created and filled (table has no associated python class)
      * openstreetmap.osm_buildings_residential is created and filled (table has no associated python class)
      * openstreetmap.osm_amenities_shops_filtered is created and filled (table has no associated python class)
      * openstreetmap.osm_buildings_with_amenities is created and filled (table has no associated python class)
      * openstreetmap.osm_buildings_without_amenities is created and filled (table has no associated python class)
      * openstreetmap.osm_amenities_not_in_buildings is created and filled (table has no associated python class)
      * openstreetmap.osm_ways_preprocessed is created and filled (table has no associated python class)
      * openstreetmap.osm_ways_with_segments is created and filled (table has no associated python class)
      * boundaries.egon_map_zensus_buildings_filtered is created and filled (table has no associated python class)
      * boundaries.egon_map_zensus_buildings_residential is created and filled (table has no associated python class)
      * openstreetmap.osm_buildings is created and filled (table has no associated python class)

    **Details and Steps**

    * Extract buildings and filter using relevant tags, e.g. residential and
      commercial, see script `osm_buildings_filter.sql` for the full list of tags.
      Resulting tables:
      * All buildings: `openstreetmap.osm_buildings`
      * Filtered buildings: `openstreetmap.osm_buildings_filtered`
      * Residential buildings: `openstreetmap.osm_buildings_residential`
        * 1st step: Filter by tags (see `osm_buildings_filter_residential.sql`)
        * 2nd step: Table is extended by finding census cells with population
          but no residential buildings and extended by commercial/retail buildings
          (see `osm_buildings_extend_residential.sql`) since they often include
          apartments as well.
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

    """

    #:
    name: str = "OsmBuildingsStreets"
    #:
    version: str = "0.0.6"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                preprocessing,
                {filter_buildings, filter_buildings_residential},
                extend_buildings_residential,
                extract_buildings_filtered_amenities,
                {
                    create_buildings_filtered_zensus_mapping,
                    create_buildings_residential_zensus_mapping,
                    create_buildings_filtered_all_zensus_mapping,
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

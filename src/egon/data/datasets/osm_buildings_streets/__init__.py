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
    db.execute_sql_script(
        os.path.join(
            os.path.dirname(__file__),
            script
        )
    )


def extract_buildings():
    sql_scripts = [
        'osm_amenities_shops_preprocessing.sql',
        'osm_buildings_filter.sql',
        'osm_buildings_zensus_mapping.sql',
        'osm_buildings_temp_tables.sql',
        'osm_buildings_amentities_results.sql'
    ]
    for script in sql_scripts:
        execute_sql_script(script)


def extract_ways():
    execute_sql_script('osm_ways_preprocessing.sql')


def drop_temp_tables():
    execute_sql_script('drop_temp_tables.sql')


def add_metadata():
    pass


class OsmBuildingsStreets(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="OsmBuildingsStreets",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(
                {extract_buildings,
                 extract_ways},
                drop_temp_tables,
                add_metadata
            ),
        )

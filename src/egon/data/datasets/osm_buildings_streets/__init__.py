import os

from egon.data import db
from egon.data.datasets import Dataset


def data_extract():
    sql_scripts = [
        'osm_amenities_shops_preprocessing.sql',
        'osm_buildings_preprocessing.sql',
        'osm_ways_preprocessing.sql',
        'drop_temp_tables.sql'
    ]
    for script in sql_scripts:
        db.execute_sql_script(
            os.path.join(
                os.path.dirname(__file__),
                script
            )
        )


def add_metadata():
    pass


class OsmBuildingsStreets(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="OsmBuildingsStreets",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(data_extract, add_metadata),
        )

from geoalchemy2.shape import to_shape
from sqlalchemy import Integer, Table, func, inspect
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.electricity_demand.temporal import (
    calc_load_curve,
    calc_load_curves_cts,
)
from egon.data.datasets.electricity_demand_timeseries.hh_buildings import (
    generate_synthetic_buildings,
)
from egon.data.datasets.electricity_demand_timeseries.tools import (
    random_ints_until_sum,
)
import egon.data.config

engine = db.engine()
Base = declarative_base()

data_config = egon.data.config.datasets()
RANDOM_SEED = egon.data.config.settings()["egon-data"]["--random-seed"]


import saio


def synthetic_buildings_for_amenities():
    """
    Synthetic buildings are generated for amenities which could not be
    allocated to a building. Buildings are randomly spread within census cells.
    The Number of buildings is derived from amenity count and randomly chosen
    number of amenities per building <= 3.

    Returns
    -------
    pd.DataFrame
        Table of synthetic buildings

    """
    # import db tables
    saio.register_schema("openstreetmap", engine=engine)
    saio.register_schema("society", engine=engine)

    from saio.openstreetmap import (
        osm_amenities_not_in_buildings,
        osm_buildings,
        osm_buildings_with_amenities,
    )
    from saio.society import destatis_zensus_population_per_ha_inside_germany

    from egon.data.datasets.electricity_demand_timeseries.hh_buildings import (
        OsmBuildingsSynthetic,
    )

    with db.session_scope() as session:
        cells_query = (
            session.query(
                destatis_zensus_population_per_ha_inside_germany.id.label(
                    "zensus_population_id"
                ),
                func.count(osm_amenities_not_in_buildings.osm_id).label(
                    "n_amenities_inside"
                ),
                #         osm_amenities_not_in_buildings.geom,
                #         destatis_zensus_population_per_ha_inside_germany.geom
            )
            .filter(
                func.st_contains(
                    destatis_zensus_population_per_ha_inside_germany.geom,
                    osm_amenities_not_in_buildings.geom,
                )
            )
            .group_by(
                destatis_zensus_population_per_ha_inside_germany.id,
                #         osm_amenities_not_in_buildings.geom
                #         destatis_zensus_population_per_ha_inside_germany.geom
            )
        )

    df_amenities_not_in_buildings = pd.read_sql(
        cells_query.statement, cells_query.session.bind
    )  # , index_col='id')

    # number of max amenities per building
    max_amenities = 3
    # amount of amenities is randomly generated within bounds (max_amenities,
    # amenities per cell)
    df_amenities_not_in_buildings[
        "n_amenities_inside"
    ] = df_amenities_not_in_buildings["n_amenities_inside"].apply(
        random_ints_until_sum, args=[max_amenities]
    )
    # df_amenities_not_in_buildings[
    #     "n_amenities_inside"
    # ] = df_amenities_not_in_buildings["n_amenities_inside"].apply(
    #     specific_int_until_sum, args=[max_amenities]
    # )
    df_amenities_not_in_buildings = df_amenities_not_in_buildings.explode(
        column="n_amenities_inside"
    )
    # building count per cell
    df_amenities_not_in_buildings["building_count"] = (
        df_amenities_not_in_buildings.groupby(
            ["zensus_population_id"]
        ).cumcount()
        + 1
    )
    # generate random synthetic buildings
    df_amenities_in_synthetic_buildings = generate_synthetic_buildings(
        df_amenities_not_in_buildings.set_index("zensus_population_id"),
        edge_length=5,
    )
    df_amenities_in_synthetic_buildings.rename(
        columns={"geom": "geom_building"}, inplace=True
    )
    # get max number of building ids from synthetic residential table
    with db.session_scope() as session:
        max_synth_residential_id = session.execute(
            func.max(OsmBuildingsSynthetic.id)
        ).scalar()
    max_synth_residential_id = int(max_synth_residential_id)
    # create sequential ids
    df_amenities_in_synthetic_buildings["egon_building_id"] = range(
        max_synth_residential_id + 1,
        max_synth_residential_id
        + df_amenities_in_synthetic_buildings.shape[0]
        + 1,
    )
    df_amenities_in_synthetic_buildings["building"] = "cts"
    # TODO remove in #772
    df_amenities_in_synthetic_buildings = (
        df_amenities_in_synthetic_buildings.rename(
            columns={
                "zensus_population_id": "cell_id",
                "egon_building_id": "id",
            }
        )
    )
    return df_amenities_in_synthetic_buildings


def buildings_with_amenities():
    """"""
    # import db tables
    saio.register_schema("openstreetmap", engine=engine)
    saio.register_schema("boundaries", engine=engine)

    from saio.boundaries import egon_map_zensus_buildings_filtered
    from saio.openstreetmap import (
        osm_amenities_not_in_buildings,
        osm_buildings,
        osm_buildings_with_amenities,
    )

    with db.session_scope() as session:
        cells_query = (
            session.query(
                # TODO maybe remove if osm_buildings_with_amenities.id exists
                osm_buildings.id.label("egon_building_id"),
                #         osm_buildings_with_amenities.id,
                #         osm_buildings_with_amenities.osm_id_building,
                osm_buildings_with_amenities.osm_id_amenity,
                osm_buildings_with_amenities.building,
                osm_buildings_with_amenities.n_amenities_inside,
                osm_buildings_with_amenities.area,
                osm_buildings_with_amenities.geom_building,
                osm_buildings_with_amenities.geom_point,
                egon_map_zensus_buildings_filtered.cell_id.label(
                    "zensus_population_id"
                ),
            )
            .filter(
                osm_buildings_with_amenities.osm_id_building
                == osm_buildings.osm_id
            )
            .filter(osm_buildings.id == egon_map_zensus_buildings_filtered.id)
        )
    df_amenities_in_buildings = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col=None
    )
    # TODO necessary?
    df_amenities_in_buildings["geom_building"] = df_amenities_in_buildings[
        "geom_building"
    ].apply(to_shape)
    df_amenities_in_buildings["geom_point"] = df_amenities_in_buildings[
        "geom_point"
    ].apply(to_shape)

    # Count amenities per building
    df_amenities_in_buildings["n_amenities_inside"] = 1
    df_amenities_in_buildings[
        "n_amenities_inside"
    ] = df_amenities_in_buildings.groupby("egon_building_id")[
        "n_amenities_inside"
    ].transform(
        "sum"
    )

    # Only keep one building for multiple amenities
    df_amenities_in_buildings = df_amenities_in_buildings.drop_duplicates(
        "egon_building_id"
    )
    df_amenities_in_buildings["building"] = "cts"
    # TODO maybe remove later
    df_amenities_in_buildings.sort_values("egon_building_id").reset_index(
        drop=True, inplace=True
    )
    df_amenities_in_buildings.rename(
        columns={"zensus_population_id": "cell_id", "egon_building_id": "id"},
        inplace=True,
    )

    return df_amenities_in_buildings


def write_synthetic_buildings_to_db(df_synthetic_buildings):
    """"""
    from egon.data.datasets.electricity_demand_timeseries.hh_buildings import (
        OsmBuildingsSynthetic,
    )

    # Write new buildings incl coord into db
    df_synthetic_buildings.to_postgis(
        "osm_buildings_synthetic",
        con=engine,
        if_exists="append",
        schema="openstreetmap",
        dtype={
            "id": OsmBuildingsSynthetic.id.type,
            "cell_id": OsmBuildingsSynthetic.cell_id.type,
            "geom": OsmBuildingsSynthetic.geom.type,
            "geom_point": OsmBuildingsSynthetic.geom_point.type,
            "n_amenities_inside": OsmBuildingsSynthetic.n_amenities_inside.type,
            "building": OsmBuildingsSynthetic.building.type,
            "area": OsmBuildingsSynthetic.area.type,
        },
    )


def cts_to_buildings():

    df_synthetic_buildings_for_amenities = synthetic_buildings_for_amenities()
    df_buildings_with_amenities = buildings_with_amenities()

    write_synthetic_buildings_to_db(df_synthetic_buildings_for_amenities)


class CtsElectricityBuildings(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="CtsElectricityBuildings",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(cts_to_buildings),
        )

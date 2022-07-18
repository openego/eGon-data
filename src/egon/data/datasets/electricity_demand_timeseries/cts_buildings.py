from geoalchemy2.shape import to_shape
from sqlalchemy import Integer, func
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.electricity_demand.temporal import calc_load_curves_cts
from egon.data.datasets.electricity_demand_timeseries.hh_buildings import (
    OsmBuildingsSynthetic,
)
from egon.data.datasets.electricity_demand_timeseries.tools import (
    random_ints_until_sum,
    random_point_in_square,
    specific_int_until_sum,
)
import egon.data.config

engine = db.engine()
Base = declarative_base()

data_config = egon.data.config.datasets()
RANDOM_SEED = egon.data.config.settings()["egon-data"]["--random-seed"]

import saio

# import db tables
saio.register_schema("openstreetmap", engine=engine)
saio.register_schema("society", engine=engine)
saio.register_schema("demand", engine=engine)
saio.register_schema("boundaries", engine=engine)

from saio.boundaries import egon_map_zensus_buildings_filtered_all
from saio.demand import egon_demandregio_zensus_electricity
from saio.openstreetmap import (
    osm_amenities_not_in_buildings_filtered,
    osm_amenities_shops_filtered,
    osm_buildings_filtered,
    osm_buildings_filtered_with_amenities,
    osm_buildings_synthetic,
)
from saio.society import destatis_zensus_population_per_ha


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
    saio.register_schema("demand", engine=engine)

    from saio.demand import egon_demandregio_zensus_electricity
    from saio.openstreetmap import osm_amenities_not_in_buildings_filtered
    from saio.society import destatis_zensus_population_per_ha

    with db.session_scope() as session:
        cells_query = (
            session.query(
                destatis_zensus_population_per_ha.id.label(
                    "zensus_population_id"
                ),
                # TODO can be used for square around amenity
                #  (1 geom_amenity: 1 geom_building)
                #  not unique amenity_ids yet
                osm_amenities_not_in_buildings_filtered.geom_amenity,
                osm_amenities_not_in_buildings_filtered.egon_amenity_id,
                # egon_demandregio_zensus_electricity.demand,
                # # TODO can be used to generate n random buildings
                # # (n amenities : 1 randombuilding)
                # func.count(
                #     osm_amenities_not_in_buildings_filtered.egon_amenity_id
                # ).label("n_amenities_inside"),
                # destatis_zensus_population_per_ha.geom,
            )
            .filter(
                func.st_within(
                    osm_amenities_not_in_buildings_filtered.geom_amenity,
                    destatis_zensus_population_per_ha.geom,
                )
            )
            .filter(
                destatis_zensus_population_per_ha.id
                == egon_demandregio_zensus_electricity.zensus_population_id
            )
            .filter(
                egon_demandregio_zensus_electricity.sector == "service",
                egon_demandregio_zensus_electricity.scenario == "eGon2035"
                #         ).group_by(
                #             egon_demandregio_zensus_electricity.zensus_population_id,
                #             destatis_zensus_population_per_ha.geom,
            )
        )
    # # TODO can be used to generate n random buildings
    # df_cells_with_amenities_not_in_buildings = gpd.read_postgis(
    #     cells_query.statement, cells_query.session.bind, geom_col="geom"
    # )
    #
    # # number of max amenities per building
    # max_amenities = 3
    # # amount of amenities is randomly generated within bounds (max_amenities,
    # # amenities per cell)
    # df_cells_with_amenities_not_in_buildings[
    #     "n_amenities_inside"
    # ] = df_cells_with_amenities_not_in_buildings["n_amenities_inside"].apply(
    #     random_ints_until_sum, args=[max_amenities]
    # )
    # # Specific amount of amenities per building
    # # df_amenities_not_in_buildings[
    # #     "n_amenities_inside"
    # # ] = df_amenities_not_in_buildings["n_amenities_inside"].apply(
    # #     specific_int_until_sum, args=[max_amenities]
    # # )
    #
    # # Unnest each building
    # df_synthetic_buildings_for_amenities = (
    #     df_cells_with_amenities_not_in_buildings.explode(
    #         column="n_amenities_inside"
    #     )
    # )
    # # building count per cell
    # df_synthetic_buildings_for_amenities["building_count"] = (
    #     df_synthetic_buildings_for_amenities.groupby(
    #         ["zensus_population_id"]
    #     ).cumcount()
    #     + 1
    # )
    # # generate random synthetic buildings
    # edge_length = 5
    # # create random points within census cells
    # points = random_point_in_square(
    #     geom=df_synthetic_buildings_for_amenities["geom"], tol=edge_length / 2
    # )
    #
    # # Store center of polygon
    # df_synthetic_buildings_for_amenities["geom_point"] = points
    # df_synthetic_buildings_for_amenities = (
    #     df_synthetic_buildings_for_amenities.drop(columns=["geom"])
    # )

    # # TODO can be used for square around amenity
    df_synthetic_buildings_for_amenities = gpd.read_postgis(
        cells_query.statement,
        cells_query.session.bind,
        geom_col="geom_amenity",
    )
    points = df_synthetic_buildings_for_amenities["geom_amenity"]

    # Create building using a square around point
    edge_length = 5
    df_synthetic_buildings_for_amenities["geom_building"] = points.buffer(
        distance=edge_length / 2, cap_style=3
    )

    # TODO Check CRS
    df_synthetic_buildings_for_amenities = gpd.GeoDataFrame(
        df_synthetic_buildings_for_amenities,
        crs="EPSG:4258",
        geometry="geom_building",
    )

    # TODO remove after implementation of egon_building_id
    df_synthetic_buildings_for_amenities.rename(
        columns={
            "id": "egon_building_id",
        },
        inplace=True,
    )

    # get max number of building ids from synthetic residential table
    with db.session_scope() as session:
        max_synth_residential_id = session.execute(
            func.max(OsmBuildingsSynthetic.id)
        ).scalar()
    max_synth_residential_id = int(max_synth_residential_id)

    # create sequential ids
    df_synthetic_buildings_for_amenities["egon_building_id"] = range(
        max_synth_residential_id + 1,
        max_synth_residential_id
        + df_synthetic_buildings_for_amenities.shape[0]
        + 1,
    )

    # set building type of synthetic building
    df_synthetic_buildings_for_amenities["building"] = "cts"
    # TODO remove in #772
    df_synthetic_buildings_for_amenities = (
        df_synthetic_buildings_for_amenities.rename(
            columns={
                "zensus_population_id": "cell_id",
                "egon_building_id": "id",
            }
        )
    )
    return df_synthetic_buildings_for_amenities


def buildings_with_amenities():
    """"""

    # import db tables
    saio.register_schema("openstreetmap", engine=engine)
    saio.register_schema("boundaries", engine=engine)
    saio.register_schema("demand", engine=engine)

    from saio.boundaries import egon_map_zensus_buildings_filtered_all
    from saio.demand import egon_demandregio_zensus_electricity
    from saio.openstreetmap import osm_buildings_filtered_with_amenities

    with db.session_scope() as session:
        cells_query = (
            session.query(
                osm_buildings_filtered_with_amenities.id.label(
                    "egon_building_id"
                ),
                osm_buildings_filtered_with_amenities.building,
                osm_buildings_filtered_with_amenities.n_amenities_inside,
                osm_buildings_filtered_with_amenities.area,
                osm_buildings_filtered_with_amenities.geom_building,
                osm_buildings_filtered_with_amenities.geom_point,
                egon_map_zensus_buildings_filtered_all.zensus_population_id,
            )
            .filter(
                osm_buildings_filtered_with_amenities.id
                == egon_map_zensus_buildings_filtered_all.id
            )
            .filter(
                egon_demandregio_zensus_electricity.zensus_population_id
                == egon_map_zensus_buildings_filtered_all.zensus_population_id
            )
            .filter(
                egon_demandregio_zensus_electricity.sector == "service",
                egon_demandregio_zensus_electricity.scenario == "eGon2035",
            )
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

    # # Count amenities per building
    # df_amenities_in_buildings["n_amenities_inside"] = 1
    # df_amenities_in_buildings[
    #     "n_amenities_inside"
    # ] = df_amenities_in_buildings.groupby("egon_building_id")[
    #     "n_amenities_inside"
    # ].transform(
    #     "sum"
    # )

    # # Only keep one building for multiple amenities
    # df_amenities_in_buildings = df_amenities_in_buildings.drop_duplicates(
    #     "egon_building_id"
    # )
    # df_amenities_in_buildings["building"] = "cts"
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

    # Only take existing columns
    columns = [
        column.key for column in OsmBuildingsSynthetic.__table__.columns
    ]
    df_synthetic_buildings = df_synthetic_buildings.loc[:, columns]
    # Write new buildings incl coord into db
    df_synthetic_buildings.to_postgis(
        "osm_buildings_synthetic",
        con=engine,
        if_exists="append",
        schema="openstreetmap",
        dtype={
            "id": OsmBuildingsSynthetic.id.type,
            "cell_id": OsmBuildingsSynthetic.cell_id.type,
            "geom_building": OsmBuildingsSynthetic.geom_building.type,
            "geom_point": OsmBuildingsSynthetic.geom_point.type,
            "n_amenities_inside": OsmBuildingsSynthetic.n_amenities_inside.type,
            "building": OsmBuildingsSynthetic.building.type,
            "area": OsmBuildingsSynthetic.area.type,
        },
    )


def cts_to_buildings():

    # Buildings with amenities
    df_buildings_with_amenities = buildings_with_amenities()

    # Create synthetic buildings for amenites without buildings
    df_synthetic_buildings_for_amenities = synthetic_buildings_for_amenities()
    write_synthetic_buildings_to_db(df_synthetic_buildings_for_amenities)

    # Create synthetic amenities in cells without amenities but CTS demand


class CtsElectricityBuildings(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="CtsElectricityBuildings",
            version="0.0.0.",
            dependencies=dependencies,
            tasks=(cts_to_buildings),
        )

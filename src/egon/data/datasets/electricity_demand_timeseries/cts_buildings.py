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


def amenities_without_buildings():
    """

    Returns
    -------
    pd.DataFrame
        Table of amenities without buildings

    """
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

    # # TODO can be used for square around amenity
    df_synthetic_buildings_for_amenities = gpd.read_postgis(
        cells_query.statement,
        cells_query.session.bind,
        geom_col="geom_amenity",
    )
    return df_synthetic_buildings_for_amenities


def place_buildings_with_amenities(df, amenities=None, max_amenities=None):
    """
    Building centers are placed randomly within census cells.
    The Number of buildings is derived from n_amenity_inside, the selected
    method and number of amenities per building.
    """
    if isinstance(max_amenities, int):
        # amount of amenities is randomly generated within bounds (max_amenities,
        # amenities per cell)
        df["n_amenities_inside"] = df["n_amenities_inside"].apply(
            random_ints_until_sum, args=[max_amenities]
        )
    if isinstance(amenities, int):
        # Specific amount of amenities per building
        df["n_amenities_inside"] = df["n_amenities_inside"].apply(
            specific_int_until_sum, args=[amenities]
        )

    # Unnest each building
    df = df.explode(column="n_amenities_inside")

    # building count per cell
    df["building_count"] = df.groupby(["zensus_population_id"]).cumcount() + 1

    # generate random synthetic buildings
    edge_length = 5
    # create random points within census cells
    points = random_point_in_square(geom=df["geom"], tol=edge_length / 2)

    df.reset_index(drop=True, inplace=True)
    # Store center of polygon
    df["geom_point"] = points
    # Drop geometry of census cell
    df = df.drop(columns=["geom"])

    return df


def create_synthetic_buildings(df, points=None, crs="EPSG:4258"):
    """
    Synthetic buildings are generated around points.
    """
    if isinstance(points, str) and points in df.columns:
        points = df[points]
    elif isinstance(points, gpd.GeoSeries):
        pass
    else:
        raise ValueError("Points are of the wrong type")

    # Create building using a square around point
    edge_length = 5
    df["geom_building"] = points.buffer(distance=edge_length / 2, cap_style=3)

    # TODO Check CRS
    df = gpd.GeoDataFrame(
        df,
        crs=crs,
        geometry="geom_building",
    )

    # TODO remove after implementation of egon_building_id
    df.rename(columns={"id": "egon_building_id"}, inplace=True)

    # get max number of building ids from synthetic residential table
    with db.session_scope() as session:
        max_synth_residential_id = session.execute(
            func.max(OsmBuildingsSynthetic.id)
        ).scalar()
    max_synth_residential_id = int(max_synth_residential_id)

    # create sequential ids
    df["egon_building_id"] = range(
        max_synth_residential_id + 1,
        max_synth_residential_id + df.shape[0] + 1,
    )

    df["area"] = df["geom_building"].area
    # set building type of synthetic building
    df["building"] = "cts"
    # TODO remove in #772
    df = df.rename(
        columns={
            "zensus_population_id": "cell_id",
            "egon_building_id": "id",
        }
    )
    return df


def buildings_with_amenities():
    """"""
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

    if not "geom_point" in df_synthetic_buildings.columns:
        df_synthetic_buildings["geom_point"] = df_synthetic_buildings[
            "geom_building"
        ].centroid
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


def buildings_without_amenities():
    """ """
    # buildings_filtered in cts-demand-cells without amenities
    with db.session_scope() as session:

        # Synthetic Buildings
        q_synth_buildings = session.query(
            osm_buildings_synthetic.cell_id.cast(Integer).label(
                "zensus_population_id"
            ),
            osm_buildings_synthetic.id.cast(Integer).label("id"),
            osm_buildings_synthetic.area.label("area"),
            osm_buildings_synthetic.geom_building.label("geom_building"),
            osm_buildings_synthetic.geom_point.label("geom_point"),
        )

        # Buildings filtered
        q_buildings_filtered = session.query(
            egon_map_zensus_buildings_filtered_all.zensus_population_id,
            osm_buildings_filtered.id,
            osm_buildings_filtered.area,
            osm_buildings_filtered.geom_building,
            osm_buildings_filtered.geom_point,
        ).filter(
            osm_buildings_filtered.id
            == egon_map_zensus_buildings_filtered_all.id
        )

        # Amenities + zensus_population_id
        q_amenities = (
            session.query(
                destatis_zensus_population_per_ha.id.label(
                    "zensus_population_id"
                ),
            )
            .filter(
                func.st_within(
                    osm_amenities_shops_filtered.geom_amenity,
                    destatis_zensus_population_per_ha.geom,
                )
            )
            .distinct(destatis_zensus_population_per_ha.id)
        )

        # Cells with CTS demand but without amenities
        q_cts_without_amenities = (
            session.query(
                egon_demandregio_zensus_electricity.zensus_population_id,
            )
            .filter(
                egon_demandregio_zensus_electricity.sector == "service",
                egon_demandregio_zensus_electricity.scenario == "eGon2035",
            )
            .filter(
                egon_demandregio_zensus_electricity.zensus_population_id.notin_(
                    q_amenities
                )
            )
            .distinct()
        )

        # Buildings filtered + synthetic buildings residential in
        # cells with CTS demand but without amenities
        cells_query = q_synth_buildings.union(q_buildings_filtered).filter(
            egon_map_zensus_buildings_filtered_all.zensus_population_id.in_(
                q_cts_without_amenities
            )
        )

    # df_buildings_without_amenities = pd.read_sql(
    #     cells_query.statement, cells_query.session.bind, index_col=None)
    df_buildings_without_amenities = gpd.read_postgis(
        cells_query.statement,
        cells_query.session.bind,
        geom_col="geom_building",
    )

    df_buildings_without_amenities = df_buildings_without_amenities.rename(
        columns={
            "zensus_population_id": "cell_id",
            "egon_building_id": "id",
        }
    )

    return df_buildings_without_amenities


def select_cts_buildings(df_buildings_without_amenities):
    """ """
    # Select one building each cell
    df_buildings_with_cts_demand = (
        df_buildings_without_amenities.drop_duplicates(
            subset="cell_id", keep="first"
        ).reset_index(drop=True)
    )
    df_buildings_with_cts_demand["n_amenities_inside"] = 1
    df_buildings_with_cts_demand["building"] = "cts"

    return df_buildings_with_cts_demand


def cells_with_cts_demand_only(df_buildings_without_amenities):
    """"""
    # cells mit amenities
    with db.session_scope() as session:
        sub_query = (
            session.query(
                destatis_zensus_population_per_ha.id.label(
                    "zensus_population_id"
                ),
            )
            .filter(
                func.st_within(
                    osm_amenities_shops_filtered.geom_amenity,
                    destatis_zensus_population_per_ha.geom,
                )
            )
            .distinct(destatis_zensus_population_per_ha.id)
        )

        cells_query = (
            session.query(
                egon_demandregio_zensus_electricity.zensus_population_id,
                egon_demandregio_zensus_electricity.scenario,
                egon_demandregio_zensus_electricity.sector,
                egon_demandregio_zensus_electricity.demand,
                destatis_zensus_population_per_ha.geom,
            )
            .filter(
                egon_demandregio_zensus_electricity.sector == "service",
                egon_demandregio_zensus_electricity.scenario == "eGon2035",
            )
            .filter(
                egon_demandregio_zensus_electricity.zensus_population_id.notin_(
                    sub_query
                )
            )
            .filter(
                egon_demandregio_zensus_electricity.zensus_population_id
                == destatis_zensus_population_per_ha.id
            )
        )

    df_cts_cell_without_amenities = gpd.read_postgis(
        cells_query.statement,
        cells_query.session.bind,
        geom_col="geom",
        index_col=None,
    )

    # TODO maybe remove
    df_buildings_without_amenities = df_buildings_without_amenities.rename(
        columns={"cell_id": "zensus_population_id"}
    )

    # Census cells with only cts demand
    df_cells_only_cts_demand = df_cts_cell_without_amenities.loc[
        ~df_cts_cell_without_amenities["zensus_population_id"].isin(
            df_buildings_without_amenities["zensus_population_id"].unique()
        )
    ]

    df_cells_only_cts_demand.reset_index(drop=True, inplace=True)

    return df_cells_only_cts_demand


def get_census_cell_share():
    """"""

    from egon.data.datasets.electricity_demand import (
        EgonDemandRegioZensusElectricity,
    )
    from egon.data.datasets.zensus_mv_grid_districts import (
        MapZensusGridDistricts,
    )

    with db.session_scope() as session:
        cells_query = (
            session.query(
                EgonDemandRegioZensusElectricity, MapZensusGridDistricts.bus_id
            )
            .filter(EgonDemandRegioZensusElectricity.sector == "service")
            .filter(EgonDemandRegioZensusElectricity.scenario == "eGon2035")
            .filter(
                EgonDemandRegioZensusElectricity.zensus_population_id
                == MapZensusGridDistricts.zensus_population_id
            )
        )

    df_demand_regio_electricity_demand = pd.read_sql(
        cells_query.statement,
        cells_query.session.bind,
        index_col="zensus_population_id",
    )

    # get demand share of cell per bus
    # share ist für scenarios identisch
    df_census_share = df_demand_regio_electricity_demand[
        "demand"
    ] / df_demand_regio_electricity_demand.groupby("bus_id")[
        "demand"
    ].transform(
        "sum"
    )

    df_census_share = pd.concat(
        [df_census_share, df_demand_regio_electricity_demand["bus_id"]], axis=1
    )

    return df_census_share


def get_cts_profiles(scenario):
    scenario = "eGon2035"
    df_cts_load_curve = calc_load_curves_cts(scenario)

    return df_cts_load_curve


def calc_building_amenity_share(df_cts_buildings):
    """"""
    df_building_amenity_share = 1 / df_cts_buildings.groupby("cell_id")[
        "n_amenities_inside"
    ].transform("sum")
    df_building_amenity_share = pd.concat(
        [
            df_building_amenity_share.rename("building_amenity_share"),
            df_cts_buildings[["cell_id", "id"]],
        ],
        axis=1,
    )
    return df_building_amenity_share


def cts_to_buildings():

    # Buildings with amenities
    df_buildings_with_amenities = buildings_with_amenities()

    # Create synthetic buildings for amenites without buildings
    df_amenities_without_buildings = amenities_without_buildings()
    df_amenities_without_buildings["n_amenities_inside"] = 1
    df_synthetic_buildings_for_amenities = create_synthetic_buildings(
        df_amenities_without_buildings, points="geom_amenity"
    )
    # write_synthetic_buildings_to_db(df_synthetic_buildings_for_amenities)

    # Cells without amenities but CTS demand and buildings
    df_buildings_without_amenities = buildings_without_amenities()
    df_buildings_without_amenities = select_cts_buildings(
        df_buildings_without_amenities
    )
    df_buildings_without_amenities["n_amenities_inside"] = 1

    # Create synthetic amenities and buildings in cells with only CTS demand
    df_cells_with_cts_demand_only = cells_with_cts_demand_only(
        df_buildings_without_amenities
    )
    # Only 1 Amenity per cell
    df_cells_with_cts_demand_only["n_amenities_inside"] = 1
    # Only 1 Amenity per Building
    df_cells_with_cts_demand_only = place_buildings_with_amenities(
        df_cells_with_cts_demand_only, amenities=1
    )
    # Leads to only 1 building per cell
    df_synthetic_buildings_for_synthetic_amenities = (
        create_synthetic_buildings(
            df_cells_with_cts_demand_only, points="geom_point"
        )
    )
    # write_synthetic_buildings_to_db(df_synthetic_buildings_for_synthetic_amenities)

    columns = ["cell_id", "id", "geom_building", "n_amenities_inside"]
    df_cts_buildings = pd.concat(
        [
            df_buildings_with_amenities[columns],
            df_synthetic_buildings_for_amenities[columns],
            df_buildings_without_amenities[columns],
            df_synthetic_buildings_for_synthetic_amenities[columns],
        ],
        axis=0,
        ignore_index=True,
    )
    # TODO maybe remove after #772
    df_cts_buildings["id"] = df_cts_buildings["id"].astype(int)

    df_building_amenity_share = calc_building_amenity_share(df_cts_buildings)

    return df_cts_buildings, df_building_amenity_share


class CtsElectricityBuildings(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="CtsElectricityBuildings",
            version="0.0.0.",
            dependencies=dependencies,
            tasks=(cts_to_buildings),
        )

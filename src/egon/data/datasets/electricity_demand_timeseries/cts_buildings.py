from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape
from sqlalchemy import REAL, Column, Integer, String, func
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd
import saio
import logging

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.electricity_demand import (
    EgonDemandRegioZensusElectricity,
)
from egon.data.datasets.electricity_demand.temporal import (
    EgonEtragoElectricityCts,
    calc_load_curves_cts,
)
from egon.data.datasets.electricity_demand_timeseries.hh_buildings import (
    BuildingPeakLoads,
    OsmBuildingsSynthetic,
)
from egon.data.datasets.electricity_demand_timeseries.tools import (
    random_ints_until_sum,
    random_point_in_square,
    specific_int_until_sum,
    write_table_to_postgis,
    write_table_to_postgres,
)
from egon.data.datasets.heat_demand import EgonPetaHeat
from egon.data.datasets.zensus_mv_grid_districts import MapZensusGridDistricts
from egon.data.datasets.zensus_vg250 import DestatisZensusPopulationPerHa

engine = db.engine()
Base = declarative_base()

# import db tables
saio.register_schema("openstreetmap", engine=engine)
saio.register_schema("boundaries", engine=engine)


class EgonCtsElectricityDemandBuildingShare(Base):
    __tablename__ = "egon_cts_electricity_demand_building_share"
    __table_args__ = {"schema": "demand"}

    id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    bus_id = Column(Integer, index=True)
    profile_share = Column(REAL)


class EgonCtsHeatDemandBuildingShare(Base):
    __tablename__ = "egon_cts_heat_demand_building_share"
    __table_args__ = {"schema": "demand"}

    id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    bus_id = Column(Integer, index=True)
    profile_share = Column(REAL)


class CtsBuildings(Base):
    __tablename__ = "egon_cts_buildings"
    __table_args__ = {"schema": "openstreetmap"}

    serial = Column(Integer, primary_key=True)
    id = Column(Integer, index=True)
    zensus_population_id = Column(Integer, index=True)
    geom_building = Column(Geometry("Polygon", 3035))
    n_amenities_inside = Column(Integer)
    source = Column(String)


def start_logging():
    """Start logging into console"""
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    logformat = logging.Formatter(
        "%(asctime)s %(message)s", "%m/%d/%Y %H:%M:%S"
    )
    sh = logging.StreamHandler()
    sh.setFormatter(logformat)
    log.addHandler(sh)
    return log


def amenities_without_buildings():
    """
    Amenities which have no buildings assigned and are in
    a cell with cts demand are determined.

    Returns
    -------
    pd.DataFrame
        Table of amenities without buildings
    """
    from saio.openstreetmap import osm_amenities_not_in_buildings_filtered

    with db.session_scope() as session:
        cells_query = (
            session.query(
                DestatisZensusPopulationPerHa.id.label("zensus_population_id"),
                # TODO can be used for square around amenity
                #  (1 geom_amenity: 1 geom_building)
                #  not unique amenity_ids yet
                osm_amenities_not_in_buildings_filtered.geom_amenity,
                osm_amenities_not_in_buildings_filtered.egon_amenity_id,
                # EgonDemandRegioZensusElectricity.demand,
                # # TODO can be used to generate n random buildings
                # # (n amenities : 1 randombuilding)
                # func.count(
                #     osm_amenities_not_in_buildings_filtered.egon_amenity_id
                # ).label("n_amenities_inside"),
                # DestatisZensusPopulationPerHa.geom,
            )
            .filter(
                func.st_within(
                    osm_amenities_not_in_buildings_filtered.geom_amenity,
                    DestatisZensusPopulationPerHa.geom,
                )
            )
            .filter(
                DestatisZensusPopulationPerHa.id
                == EgonDemandRegioZensusElectricity.zensus_population_id
            )
            .filter(
                EgonDemandRegioZensusElectricity.sector == "service",
                EgonDemandRegioZensusElectricity.scenario == "eGon2035"
                #         ).group_by(
                #             EgonDemandRegioZensusElectricity.zensus_population_id,
                #             DestatisZensusPopulationPerHa.geom,
            )
        )
    # # TODO can be used to generate n random buildings
    # df_cells_with_amenities_not_in_buildings = gpd.read_postgis(
    #     cells_query.statement, cells_query.session.bind, geom_col="geom"
    # )
    #

    # # TODO can be used for square around amenity
    df_amenities_without_buildings = gpd.read_postgis(
        cells_query.statement,
        cells_query.session.bind,
        geom_col="geom_amenity",
    )
    return df_amenities_without_buildings


def place_buildings_with_amenities(df, amenities=None, max_amenities=None):
    """
    Building centroids are placed randomly within census cells.
    The Number of buildings is derived from n_amenity_inside, the selected
    method and number of amenities per building.

    Returns
    -------
    df: gpd.GeoDataFrame
        Table of buildings centroids
    """
    if isinstance(max_amenities, int):
        # amount of amenities is randomly generated within bounds
        # (max_amenities, amenities per cell)
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


def create_synthetic_buildings(df, points=None, crs="EPSG:3035"):
    """
    Synthetic buildings are generated around points.

    Parameters
    ----------
    df: pd.DataFrame
        Table of census cells
    points: gpd.GeoSeries or str
        List of points to place buildings around or column name of df
    crs: str
        CRS of result table

    Returns
    -------
    df: gpd.GeoDataFrame
        Synthetic buildings
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

    if "geom_point" not in df.columns:
        df["geom_point"] = df["geom_building"].centroid

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
            # "zensus_population_id": "cell_id",
            "egon_building_id": "id",
        }
    )
    return df


def buildings_with_amenities():
    """
    Amenities which are assigned to buildings are determined
    and grouped per building and zensus cell. Buildings
    covering multiple cells therefore exists multiple times
    but in different zensus cells. This is necessary to cover
    all cells with a cts demand. If buildings exist in multiple
    substations, their amenities are summed and assigned and kept in
    one substation only. If as a result, a census cell is uncovered,
    a synthetic amenity is placed. The buildings are aggregated
    afterwards during the calculation of the profile_share.

    Returns
    -------
    df_buildings_with_amenities: gpd.GeoDataFrame
        Contains all buildings with amenities per zensus cell.
    df_lost_cells: gpd.GeoDataFrame
        Contains synthetic amenities in lost cells. Might be empty
    """

    from saio.openstreetmap import osm_amenities_in_buildings_filtered

    with db.session_scope() as session:
        cells_query = (
            session.query(
                osm_amenities_in_buildings_filtered,
                MapZensusGridDistricts.bus_id,
            )
            .filter(
                MapZensusGridDistricts.zensus_population_id
                == osm_amenities_in_buildings_filtered.zensus_population_id
            )
            .filter(
                EgonDemandRegioZensusElectricity.zensus_population_id
                == osm_amenities_in_buildings_filtered.zensus_population_id
            )
            .filter(
                EgonDemandRegioZensusElectricity.sector == "service",
                EgonDemandRegioZensusElectricity.scenario == "eGon2035",
            )
        )
    df_amenities_in_buildings = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col=None
    )

    df_amenities_in_buildings["geom_building"] = df_amenities_in_buildings[
        "geom_building"
    ].apply(to_shape)
    df_amenities_in_buildings["geom_amenity"] = df_amenities_in_buildings[
        "geom_amenity"
    ].apply(to_shape)

    df_amenities_in_buildings["n_amenities_inside"] = 1

    # add identifier column for buildings in multiple substations
    df_amenities_in_buildings[
        "duplicate_identifier"
    ] = df_amenities_in_buildings.groupby(["id", "bus_id"])[
        "n_amenities_inside"
    ].transform(
        "cumsum"
    )
    df_amenities_in_buildings = df_amenities_in_buildings.sort_values(
        ["id", "duplicate_identifier"]
    )
    # sum amenities of buildings with multiple substations
    df_amenities_in_buildings[
        "n_amenities_inside"
    ] = df_amenities_in_buildings.groupby(["id", "duplicate_identifier"])[
        "n_amenities_inside"
    ].transform(
        "sum"
    )

    # create column to always go for bus_id with max amenities
    df_amenities_in_buildings[
        "max_amenities"
    ] = df_amenities_in_buildings.groupby(["id", "bus_id"])[
        "n_amenities_inside"
    ].transform(
        "sum"
    )
    # sort to go for
    df_amenities_in_buildings.sort_values(
        ["id", "max_amenities"], ascending=False, inplace=True
    )

    # identify lost zensus cells
    df_lost_cells = df_amenities_in_buildings.loc[
        df_amenities_in_buildings.duplicated(
            subset=["id", "duplicate_identifier"], keep="first"
        )
    ]
    df_lost_cells.drop_duplicates(
        subset=["zensus_population_id"], inplace=True
    )

    # drop buildings with multiple substation and lower max amenity
    df_amenities_in_buildings.drop_duplicates(
        subset=["id", "duplicate_identifier"], keep="first", inplace=True
    )

    # check if lost zensus cells are already covered
    if not df_lost_cells.empty:
        if not (
            df_amenities_in_buildings["zensus_population_id"]
            .isin(df_lost_cells["zensus_population_id"])
            .empty
        ):
            # query geom data for cell if not
            with db.session_scope() as session:
                cells_query = session.query(
                    DestatisZensusPopulationPerHa.id,
                    DestatisZensusPopulationPerHa.geom,
                ).filter(
                    DestatisZensusPopulationPerHa.id.in_(
                        df_lost_cells["zensus_population_id"]
                    )
                )

            df_lost_cells = gpd.read_postgis(
                cells_query.statement,
                cells_query.session.bind,
                geom_col="geom",
            )
            # TODO maybe adapt method
            # place random amenity in cell
            df_lost_cells["n_amenities_inside"] = 1
            df_lost_cells.rename(
                columns={
                    "id": "zensus_population_id",
                },
                inplace=True,
            )
            df_lost_cells = place_buildings_with_amenities(
                df_lost_cells, amenities=1
            )
            df_lost_cells.rename(
                columns={
                    # "id": "zensus_population_id",
                    "geom_point": "geom_amenity",
                },
                inplace=True,
            )
            df_lost_cells.drop(
                columns=["building_count", "n_amenities_inside"], inplace=True
            )
        else:
            df_lost_cells = None
    else:
        df_lost_cells = None

    # drop helper columns
    df_amenities_in_buildings.drop(
        columns=["duplicate_identifier"], inplace=True
    )

    # sum amenities per building and cell
    df_amenities_in_buildings[
        "n_amenities_inside"
    ] = df_amenities_in_buildings.groupby(["zensus_population_id", "id"])[
        "n_amenities_inside"
    ].transform(
        "sum"
    )
    # drop duplicated buildings
    df_buildings_with_amenities = df_amenities_in_buildings.drop_duplicates(
        ["id", "zensus_population_id"]
    )
    df_buildings_with_amenities.reset_index(inplace=True, drop=True)

    df_buildings_with_amenities = df_buildings_with_amenities[
        ["id", "zensus_population_id", "geom_building", "n_amenities_inside"]
    ]
    df_buildings_with_amenities.rename(
        columns={
            # "zensus_population_id": "cell_id",
            "egon_building_id": "id"
        },
        inplace=True,
    )

    return df_buildings_with_amenities, df_lost_cells


def buildings_without_amenities():
    """
    Buildings (filtered and synthetic) in cells with
    cts demand but no amenities are determined.

    Returns
    -------
    df_buildings_without_amenities: gpd.GeoDataFrame
        Table of buildings without amenities in zensus cells
        with cts demand.
    """
    from saio.boundaries import egon_map_zensus_buildings_filtered_all
    from saio.openstreetmap import (
        osm_amenities_shops_filtered,
        osm_buildings_filtered,
        osm_buildings_synthetic,
    )

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
                DestatisZensusPopulationPerHa.id.label("zensus_population_id"),
            )
            .filter(
                func.st_within(
                    osm_amenities_shops_filtered.geom_amenity,
                    DestatisZensusPopulationPerHa.geom,
                )
            )
            .distinct(DestatisZensusPopulationPerHa.id)
        )

        # Cells with CTS demand but without amenities
        q_cts_without_amenities = (
            session.query(
                EgonDemandRegioZensusElectricity.zensus_population_id,
            )
            .filter(
                EgonDemandRegioZensusElectricity.sector == "service",
                EgonDemandRegioZensusElectricity.scenario == "eGon2035",
            )
            .filter(
                EgonDemandRegioZensusElectricity.zensus_population_id.notin_(
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
            # "zensus_population_id": "cell_id",
            "egon_building_id": "id",
        }
    )

    return df_buildings_without_amenities


def select_cts_buildings(df_buildings_wo_amenities, max_n):
    """
    N Buildings (filtered and synthetic) in each cell with
    cts demand are selected. Only the first n buildings
    are taken for each cell. The buildings are sorted by surface
    area.

    Returns
    -------
    df_buildings_with_cts_demand: gpd.GeoDataFrame
        Table of buildings
    """

    df_buildings_wo_amenities.sort_values(
        "area", ascending=False, inplace=True
    )
    # select first n ids each census cell if available
    df_buildings_with_cts_demand = (
        df_buildings_wo_amenities.groupby("zensus_population_id")
        .nth(list(range(max_n)))
        .reset_index()
    )
    df_buildings_with_cts_demand.reset_index(drop=True, inplace=True)

    return df_buildings_with_cts_demand


def cells_with_cts_demand_only(df_buildings_without_amenities):
    """
    Cells with cts demand but no amenities or buildilngs
    are determined.

    Returns
    -------
    df_cells_only_cts_demand: gpd.GeoDataFrame
        Table of cells with cts demand but no amenities or buildings
    """
    from saio.openstreetmap import osm_amenities_shops_filtered

    # cells mit amenities
    with db.session_scope() as session:
        sub_query = (
            session.query(
                DestatisZensusPopulationPerHa.id.label("zensus_population_id"),
            )
            .filter(
                func.st_within(
                    osm_amenities_shops_filtered.geom_amenity,
                    DestatisZensusPopulationPerHa.geom,
                )
            )
            .distinct(DestatisZensusPopulationPerHa.id)
        )

        cells_query = (
            session.query(
                EgonDemandRegioZensusElectricity.zensus_population_id,
                EgonDemandRegioZensusElectricity.scenario,
                EgonDemandRegioZensusElectricity.sector,
                EgonDemandRegioZensusElectricity.demand,
                DestatisZensusPopulationPerHa.geom,
            )
            .filter(
                EgonDemandRegioZensusElectricity.sector == "service",
                EgonDemandRegioZensusElectricity.scenario == "eGon2035",
            )
            .filter(
                EgonDemandRegioZensusElectricity.zensus_population_id.notin_(
                    sub_query
                )
            )
            .filter(
                EgonDemandRegioZensusElectricity.zensus_population_id
                == DestatisZensusPopulationPerHa.id
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


def calc_census_cell_share(scenario="eGon2035", sector="electricity"):
    """
    The profile share for each census cell is calculated by it's
    share of annual demand per substation bus. The annual demand
    per cell is defined by DemandRegio/Peta5. The share is for both
    scenarios identical as the annual demand is linearly scaled.

    Parameters
    ----------
    scenario: str
        Scenario for which the share is calculated.
    sector: str
        Scenario for which the share is calculated.

    Returns
    -------
    df_census_share: pd.DataFrame
    """
    if sector == "electricity":
        demand_table = EgonDemandRegioZensusElectricity
    elif sector == "heat":
        demand_table = EgonPetaHeat

    with db.session_scope() as session:
        cells_query = (
            session.query(demand_table, MapZensusGridDistricts.bus_id)
            .filter(demand_table.sector == "service")
            .filter(demand_table.scenario == scenario)
            .filter(
                demand_table.zensus_population_id
                == MapZensusGridDistricts.zensus_population_id
            )
        )

    df_demand = pd.read_sql(
        cells_query.statement,
        cells_query.session.bind,
        index_col="zensus_population_id",
    )

    # get demand share of cell per bus
    df_census_share = df_demand["demand"] / df_demand.groupby("bus_id")[
        "demand"
    ].transform("sum")
    df_census_share = df_census_share.rename("cell_share")

    df_census_share = pd.concat(
        [
            df_census_share,
            df_demand[["bus_id", "scenario"]],
        ],
        axis=1,
    )

    df_census_share.reset_index(inplace=True)
    return df_census_share


def calc_building_demand_profile_share(
    df_cts_buildings, scenario="eGon2035", sector="electricity"
):
    """
    Share of cts electricity demand profile per bus for every selected building
    is calculated. Building-amenity share is multiplied with census cell share
    to get the substation bus profile share for each building. The share is
    grouped and aggregated per building as some cover multiple cells.

    Parameters
    ----------
    df_cts_buildings: gpd.GeoDataFrame
        Table of all buildings with cts demand assigned
    scenario: str
        Scenario for which the share is calculated.
    sector: str
        Sector for which the share is calculated.

    Returns
    -------
    df_building_share: pd.DataFrame
        Table of bus profile share per building

    """

    def calc_building_amenity_share(df_cts_buildings):
        """
        Calculate the building share by the number amenities per building
        within a census cell.
        """
        df_building_amenity_share = df_cts_buildings[
            "n_amenities_inside"
        ] / df_cts_buildings.groupby("zensus_population_id")[
            "n_amenities_inside"
        ].transform(
            "sum"
        )
        df_building_amenity_share = pd.concat(
            [
                df_building_amenity_share.rename("building_amenity_share"),
                df_cts_buildings[["zensus_population_id", "id"]],
            ],
            axis=1,
        )
        return df_building_amenity_share

    df_building_amenity_share = calc_building_amenity_share(df_cts_buildings)

    df_census_cell_share = calc_census_cell_share(
        scenario=scenario, sector=sector
    )

    df_demand_share = pd.merge(
        left=df_building_amenity_share,
        right=df_census_cell_share,
        left_on="zensus_population_id",
        right_on="zensus_population_id",
    )
    df_demand_share["profile_share"] = df_demand_share[
        "building_amenity_share"
    ].multiply(df_demand_share["cell_share"])

    df_demand_share = df_demand_share[
        ["id", "bus_id", "scenario", "profile_share"]
    ]
    # TODO adapt groupby?
    # Group and aggregate per building for multi cell buildings
    df_demand_share = (
        df_demand_share.groupby(["scenario", "id", "bus_id"])
        .sum()
        .reset_index()
    )
    if df_demand_share.duplicated("id", keep=False).any():
        print(
            df_demand_share.loc[df_demand_share.duplicated("id", keep=False)]
        )
    return df_demand_share


def calc_building_profiles(
    egon_building_id=None,
    bus_id=None,
    scenario="eGon2035",
    sector="electricity",
):
    """
    Calculate the demand profile for each building. The profile is
    calculated by the demand share of the building per substation bus.

    Parameters
    ----------
    egon_building_id: int
        Id of the building for which the profile is calculated.
        If not given, the profiles are calculated for all buildings.
    bus_id: int
        Id of the substation for which the all profiles are calculated.
        If not given, the profiles are calculated for all buildings.
    scenario: str
        Scenario for which the share is calculated.
    sector: str
        Sector for which the share is calculated.

    Returns
    -------
    df_building_profiles: pd.DataFrame
        Table of demand profile per building
    """
    if sector == "electricity":
        with db.session_scope() as session:
            cells_query = session.query(
                EgonCtsElectricityDemandBuildingShare,
            ).filter(
                EgonCtsElectricityDemandBuildingShare.scenario == scenario
            )

        df_demand_share = pd.read_sql(
            cells_query.statement, cells_query.session.bind, index_col=None
        )

        # TODO maybe use demand.egon_etrago_electricity_cts
        # with db.session_scope() as session:
        #     cells_query = (
        #         session.query(
        #             EgonEtragoElectricityCts
        #         ).filter(
        #             EgonEtragoElectricityCts.scn_name == scenario)
        #     )
        #
        # df_cts_profiles = pd.read_sql(
        #     cells_query.statement,
        #     cells_query.session.bind,
        # )
        # df_cts_profiles = pd.DataFrame.from_dict(
        #   df_cts_profiles.set_index('bus_id')['p_set'].to_dict(),
        #   orient="index")
        df_cts_profiles = calc_load_curves_cts(scenario)

    elif sector == "heat":
        with db.session_scope() as session:
            cells_query = session.query(
                EgonCtsHeatDemandBuildingShare,
            ).filter(EgonCtsHeatDemandBuildingShare.scenario == scenario)

        df_demand_share = pd.read_sql(
            cells_query.statement, cells_query.session.bind, index_col=None
        )

        # TODO cts heat substation profiles missing

    # get demand share of selected building id
    if isinstance(egon_building_id, int):
        if egon_building_id in df_demand_share["id"]:
            df_demand_share = df_demand_share.loc[
                df_demand_share["id"] == egon_building_id
            ]
        else:
            raise KeyError(f"Building with id {egon_building_id} not found")
    # TODO maybe add list
    # elif isinstance(egon_building_id, list):

    # get demand share of all buildings for selected bus id
    if isinstance(bus_id, int):
        if bus_id in df_demand_share["bus_id"]:
            df_demand_share = df_demand_share.loc[
                df_demand_share["bus_id"] == bus_id
            ]
        else:
            raise KeyError(f"Bus with id {bus_id} not found")

    # get demand profile for all buildings for selected demand share
    # TODO takes a few seconds per iteration
    df_building_profiles = pd.DataFrame()
    for bus_id, df in df_demand_share.groupby("bus_id"):
        shares = df.set_index("id", drop=True)["profile_share"]
        profile = df_cts_profiles.loc[:, bus_id]
        # building_profiles = profile.apply(lambda x: x * shares)
        building_profiles = np.outer(profile, shares)
        building_profiles = pd.DataFrame(
            building_profiles, index=profile.index, columns=shares.index
        )
        df_building_profiles = pd.concat(
            [df_building_profiles, building_profiles], axis=1
        )

    return df_building_profiles


def delete_synthetic_cts_buildings():
    """
    All synthetic cts buildings are deleted from the DB. This is necessary if
    the task is run multiple times as the existing synthetic buildings
    influence the results.
    """
    # import db tables
    from saio.openstreetmap import osm_buildings_synthetic

    # cells mit amenities
    with db.session_scope() as session:
        session.query(osm_buildings_synthetic).filter(
            osm_buildings_synthetic.building == "cts"
        ).delete()


def cts_buildings():
    """
    Assigns CTS demand to buildings and calculates the respective demand
    profiles. The demand profile per substation are disaggregated per
    annual demand share of each census cell and by the number of amenities
    per building within the cell. If no building data is available,
    synthetic buildings are generated around the amenities. If no amenities
    but cts demand is available, buildings are randomly selected. If no
    building nor amenity is available, random synthetic buildings are
    generated. The demand share is stored in the database.

    Note:
    -----
    Cells with CTS demand, amenities and buildings do not change within
    the scenarios, only the demand itself. Therefore scenario eGon2035
    can be used universally to determine the cts buildings but not for
    he demand share.
    """

    log = start_logging()
    log.info("Start logging!")
    # Buildings with amenities
    df_buildings_with_amenities, df_lost_cells = buildings_with_amenities()
    log.info("Buildings with amenities selected!")

    # Median number of amenities per cell
    median_n_amenities = int(
        df_buildings_with_amenities.groupby("zensus_population_id")[
            "n_amenities_inside"
        ]
        .sum()
        .median()
    )
    # TODO remove
    print(f"Median amenity value: {median_n_amenities}")

    # Remove synthetic CTS buildings if existing
    delete_synthetic_cts_buildings()
    log.info("Old synthetic cts buildings deleted!")

    # Amenities not assigned to buildings
    df_amenities_without_buildings = amenities_without_buildings()
    log.info("Amenities without buildlings selected!")

    # Append lost cells due to duplicated ids, to cover all demand cells
    if df_lost_cells.empty:

        df_lost_cells["amenities"] = median_n_amenities
        # create row for every amenity
        df_lost_cells["amenities"] = (
            df_lost_cells["amenities"].astype(int).apply(range)
        )
        df_lost_cells = df_lost_cells.explode("amenities")
        df_lost_cells.drop(columns="amenities", inplace=True)
        df_amenities_without_buildings = df_amenities_without_buildings.append(
            df_lost_cells, ignore_index=True
        )
        log.info("Lost cells due to substation intersection appended!")

    # One building per amenity
    df_amenities_without_buildings["n_amenities_inside"] = 1
    # Create synthetic buildings for amenites without buildings
    df_synthetic_buildings_with_amenities = create_synthetic_buildings(
        df_amenities_without_buildings, points="geom_amenity"
    )
    log.info("Synthetic buildings created!")

    # TODO write to DB and remove renaming
    write_table_to_postgis(
        df_synthetic_buildings_with_amenities.rename(
            columns={
                "zensus_population_id": "cell_id",
                "egon_building_id": "id",
            }
        ),
        OsmBuildingsSynthetic,
        drop=False,
    )
    log.info("Synthetic buildings exported to DB!")

    # Cells without amenities but CTS demand and buildings
    df_buildings_without_amenities = buildings_without_amenities()
    log.info("Buildings without amenities in demand cells identified!")

    # TODO Fix Adhoc Bugfix duplicated buildings
    # drop building ids which have already been used
    mask = df_buildings_without_amenities.loc[
        df_buildings_without_amenities["id"].isin(
            df_buildings_with_amenities["id"]
        )
    ].index
    df_buildings_without_amenities = df_buildings_without_amenities.drop(
        index=mask
    ).reset_index(drop=True)
    log.info(f"{mask.sum()} duplicated ids removed!")

    # select median n buildings per cell
    df_buildings_without_amenities = select_cts_buildings(
        df_buildings_without_amenities, max_n=median_n_amenities
    )
    df_buildings_without_amenities["n_amenities_inside"] = 1
    log.info(f"{median_n_amenities} buildings per cell selected!")

    # Create synthetic amenities and buildings in cells with only CTS demand
    df_cells_with_cts_demand_only = cells_with_cts_demand_only(
        df_buildings_without_amenities
    )
    log.info("Cells with only demand identified!")

    # Median n Amenities per cell
    df_cells_with_cts_demand_only["amenities"] = median_n_amenities
    # create row for every amenity
    df_cells_with_cts_demand_only["amenities"] = (
        df_cells_with_cts_demand_only["amenities"].astype(int).apply(range)
    )
    df_cells_with_cts_demand_only = df_cells_with_cts_demand_only.explode(
        "amenities"
    )
    df_cells_with_cts_demand_only.drop(columns="amenities", inplace=True)

    # Only 1 Amenity per Building
    df_cells_with_cts_demand_only["n_amenities_inside"] = 1
    df_cells_with_cts_demand_only = place_buildings_with_amenities(
        df_cells_with_cts_demand_only, amenities=1
    )
    df_synthetic_buildings_without_amenities = create_synthetic_buildings(
        df_cells_with_cts_demand_only, points="geom_point"
    )
    log.info(f"{median_n_amenities} synthetic buildings per cell created")

    # TODO write to DB and remove (backup) renaming
    write_table_to_postgis(
        df_synthetic_buildings_without_amenities.rename(
            columns={
                "zensus_population_id": "cell_id",
                "egon_building_id": "id",
            }
        ),
        OsmBuildingsSynthetic,
        drop=False,
    )
    log.info("Synthetic buildings exported to DB")

    # Concat all buildings
    columns = [
        "zensus_population_id",
        "id",
        "geom_building",
        "n_amenities_inside",
        "source",
    ]

    df_buildings_with_amenities["source"] = "bwa"
    df_synthetic_buildings_with_amenities["source"] = "sbwa"
    df_buildings_without_amenities["source"] = "bwoa"
    df_synthetic_buildings_without_amenities["source"] = "sbwoa"

    df_cts_buildings = pd.concat(
        [
            df_buildings_with_amenities[columns],
            df_synthetic_buildings_with_amenities[columns],
            df_buildings_without_amenities[columns],
            df_synthetic_buildings_without_amenities[columns],
        ],
        axis=0,
        ignore_index=True,
    )
    # TODO maybe remove after #772
    df_cts_buildings["id"] = df_cts_buildings["id"].astype(int)

    # Write table to db for debugging
    # TODO remove later
    df_cts_buildings = gpd.GeoDataFrame(
        df_cts_buildings, geometry="geom_building", crs=3035
    )
    df_cts_buildings = df_cts_buildings.reset_index().rename(
        columns={"index": "serial"}
    )
    write_table_to_postgis(
        df_cts_buildings,
        CtsBuildings,
        drop=True,
    )
    log.info("CTS buildings exported to DB!")


def cts_electricity():
    """
    Calculate cts electricity demand share of hvmv substation profile
     for buildings.
    """
    log = start_logging()
    log.info("Start logging!")
    with db.session_scope() as session:
        cells_query = session.query(CtsBuildings)

    df_cts_buildings = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col=None
    )
    log.info("CTS buildings from DB imported!")
    df_demand_share_2035 = calc_building_demand_profile_share(
        df_cts_buildings, scenario="eGon2035", sector="electricity"
    )
    log.info("Profile share for egon2035 calculated!")
    df_demand_share_100RE = calc_building_demand_profile_share(
        df_cts_buildings, scenario="eGon100RE", sector="electricity"
    )
    log.info("Profile share for egon100RE calculated!")
    df_demand_share = pd.concat(
        [df_demand_share_2035, df_demand_share_100RE],
        axis=0,
        ignore_index=True,
    )

    write_table_to_postgres(
        df_demand_share, EgonCtsElectricityDemandBuildingShare, drop=True
    )
    log.info("Profile share exported to DB!")


def cts_heat():
    """
    Calculate cts electricity demand share of hvmv substation profile
     for buildings.
    """
    log = start_logging()
    log.info("Start logging!")
    with db.session_scope() as session:
        cells_query = session.query(CtsBuildings)

    df_cts_buildings = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col=None
    )
    log.info("CTS buildings from DB imported!")

    df_demand_share_2035 = calc_building_demand_profile_share(
        df_cts_buildings, scenario="eGon2035", sector="heat"
    )
    log.info("Profile share for egon2035 calculated!")
    df_demand_share_100RE = calc_building_demand_profile_share(
        df_cts_buildings, scenario="eGon100RE", sector="heat"
    )
    log.info("Profile share for egon100RE calculated!")
    df_demand_share = pd.concat(
        [df_demand_share_2035, df_demand_share_100RE],
        axis=0,
        ignore_index=True,
    )

    write_table_to_postgres(
        df_demand_share, EgonCtsHeatDemandBuildingShare, drop=True
    )
    log.info("Profile share exported to DB!")


def get_cts_electricity_peak_load():
    """
    Get peak load of all CTS buildings for both scenarios and store in DB.
    """
    log = start_logging()
    log.info("Start logging!")
    # Delete rows with cts demand
    with db.session_scope() as session:
        session.query(BuildingPeakLoads).filter(
            BuildingPeakLoads.sector == "cts"
        ).delete()
    log.info("CTS Peak load removed from DB!")

    for scenario in ["eGon2035", "eGon100RE"]:
        df_building_profiles = calc_building_profiles(scenario=scenario)
        log.info(f"Profiles for {scenario} calculated!")
        df_peak_load = df_building_profiles.max(axis=0).rename(scenario)
        log.info("Peak load determined!")
        df_peak_load = df_peak_load.reset_index()

        # TODO rename table column to egon_building_id
        df_peak_load.rename(columns={"id": "building_id"}, inplace=True)
        df_peak_load["type"] = "cts"
        df_peak_load = df_peak_load.melt(
            id_vars=["building_id", "type"],
            var_name="scenario",
            value_name="peak_load_in_w",
        )
        # Convert unit to W
        df_peak_load["peak_load_in_w"] = df_peak_load["peak_load_in_w"] * 1e6

        # Write peak loads into db
        with db.session_scope() as session:
            session.bulk_insert_mappings(
                BuildingPeakLoads,
                df_peak_load.to_dict(orient="records"),
            )
        log.info("Peak load exported to DB!")


class CtsElectricityBuildings(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="CtsElectricityBuildings",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(
                cts_buildings,
                {cts_electricity, cts_heat},
                get_cts_electricity_peak_load,
            ),
        )

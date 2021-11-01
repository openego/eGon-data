"""
Building assignment for Household electricity demand profiles


Notes
-----
"""
from functools import partial

from geoalchemy2 import Geometry
from shapely.geometry import Point
from sqlalchemy import ARRAY, REAL, Column, Integer, String, Table, inspect
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.electricity_demand_timeseries.hh_profiles import (
    HouseholdElectricityProfilesInCensusCells,
)
import egon.data.config

engine = db.engine()
Base = declarative_base()

data_config = egon.data.config.datasets()
RANDOM_SEED = egon.data.config.settings()["egon-data"]["--random-seed"]


class HouseholdElectricityProfilesOfBuildings(Base):
    # class HouseholdElectricityProfilesInBuilding(Base):
    #     __tablename__ = "egon_household_electricity_profile_in_building"
    __tablename__ = "egon_household_electricity_profile_of_buildings"
    __table_args__ = {"schema": "demand"}

    id = Column(Integer, primary_key=True)
    cell_osm_ids = Column(String)  # , primary_key=True)
    cell_id = Column(Integer)
    # grid_id = Column(String)
    cell_profile_ids = Column(ARRAY(String, dimensions=1))


class OsmBuildingsSynthetic(Base):
    __tablename__ = "osm_buildings_synthetic"
    __table_args__ = {"schema": "openstreetmap"}

    osm_id = Column(String, primary_key=True)
    geom = Column(Geometry("Polygon", 3035), index=True)
    geom_point = Column(Geometry("POINT", 3035))
    grid_id = Column(String(16))
    cell_id = Column(String)
    building = Column(String(11))
    area = Column(REAL)


def match_osm_and_zensus_data(
    egon_household_electricity_profile_in_zensus_cell,
    egon_map_zensus_buildings_filtered,
):
    """
    Compares OSM buildings and census hh demand profiles.

    OSM building data and hh demand profiles based on census data is compared.
    Census cells with only profiles but no osm-ids are identified to generate
    synthetic buildings. Census building count is used, if available, to define
    number of missing buildings. Otherwise, the overall mean profile/building
    rate is used to derive the number of buildings from the number of already
    generated demand profiles.

    Parameters
    ----------
    egon_household_electricity_profile_in_zensus_cell: pd.DataFrame
        Table mapping hh demand profiles to census cells

    egon_map_zensus_buildings_filtered: pd.DataFrame
        Table with buildings osm-id and cell_id

    Returns
    -------
    pd.DataFrame
        Table with cell_ids and number of missing buildings
    """
    # count number of profiles for each cell
    profiles_per_cell = egon_household_electricity_profile_in_zensus_cell.cell_profile_ids.apply(
        len
    )
    # add cell_id
    number_of_buildings_profiles_per_cell = pd.merge(
        left=profiles_per_cell,
        right=egon_household_electricity_profile_in_zensus_cell["cell_id"],
        left_index=True,
        right_index=True,
    )

    # count buildings/osm_ids for each ell
    buildings_per_cell = egon_map_zensus_buildings_filtered.groupby(
        "cell_id"
    ).osm_id.count()
    buildings_per_cell = buildings_per_cell.rename("osm_ids")

    # add buildings left join to have all the cells with assigned profiles
    number_of_buildings_profiles_per_cell = pd.merge(
        left=number_of_buildings_profiles_per_cell,
        right=buildings_per_cell,
        left_on="cell_id",
        right_index=True,
        how="left",
    )

    # identify cell ids with profiles but no buildings
    number_of_buildings_profiles_per_cell = (
        number_of_buildings_profiles_per_cell.fillna(0).astype(int)
    )
    missing_buildings = number_of_buildings_profiles_per_cell.loc[
        number_of_buildings_profiles_per_cell.osm_ids == 0,
        ["cell_id", "cell_profile_ids"],
    ].set_index("cell_id")

    # query zensus building count
    egon_destatis_building_count = Table(
        "egon_destatis_zensus_apartment_building_population_per_ha",
        Base.metadata,
        schema="society",
    )
    # get table metadata from db by name and schema
    inspect(engine).reflecttable(egon_destatis_building_count, None)

    with db.session_scope() as session:
        cells_query = session.query(
            egon_destatis_building_count.c.zensus_population_id,
            egon_destatis_building_count.c.building_count,
        )

    egon_destatis_building_count = pd.read_sql(
        cells_query.statement,
        cells_query.session.bind,
        index_col="zensus_population_id",
    )
    egon_destatis_building_count = egon_destatis_building_count.dropna()

    missing_buildings = pd.merge(
        left=missing_buildings,
        right=egon_destatis_building_count,
        left_index=True,
        right_index=True,
        how="left",
    )

    # exclude cells without buildings
    only_cells_with_buildings = (
        number_of_buildings_profiles_per_cell["osm_ids"] != 0
    )
    # get profile/building rate for each cell
    profile_building_rate = (
        number_of_buildings_profiles_per_cell.loc[
            only_cells_with_buildings, "cell_profile_ids"
        ]
        / number_of_buildings_profiles_per_cell.loc[
            only_cells_with_buildings, "osm_ids"
        ]
    )

    # prepare values for missing building counts by number of profile ids
    building_count_fillna = missing_buildings.loc[
        missing_buildings["building_count"].isna(), "cell_profile_ids"
    ]
    # devide by mean profile/building rate
    building_count_fillna = (
        building_count_fillna / profile_building_rate.mean()
    )
    # replace missing building counts
    missing_buildings["building_count"] = missing_buildings[
        "building_count"
    ].fillna(value=building_count_fillna)

    # round and make type int
    missing_buildings = missing_buildings.round().astype(int)
    # generate list of building ids for each cell
    missing_buildings["building_count"] = missing_buildings[
        "building_count"
    ].apply(range)
    missing_buildings = missing_buildings.explode(column="building_count")

    return missing_buildings


def generate_synthetic_buildings(missing_buildings, edge_length):
    """
    Generate synthetic square buildings in census cells.

    Generate random placed synthetic buildings incl geom data within the bounds
    of the cencus cell. Buildings have each a square area with edge_length^2.


    Parameters
    ----------
    missing_buildings: pd.DataFrame
        Table with cell_ids and number of missing buildings
    edge_length: int
        Edge length of square synthetic building in meter

    Returns
    -------
    pd.DataFrame
        Table with generated synthetic buildings, area, cell_id and geom data

    """
    destatis_zensus_population_per_ha_inside_germany = Table(
        "destatis_zensus_population_per_ha_inside_germany",
        Base.metadata,
        schema="society",
    )
    # get table metadata from db by name and schema
    inspect(engine).reflecttable(
        destatis_zensus_population_per_ha_inside_germany, None
    )

    with db.session_scope() as session:
        cells_query = session.query(
            destatis_zensus_population_per_ha_inside_germany
        ).filter(
            destatis_zensus_population_per_ha_inside_germany.c.id.in_(
                missing_buildings.index
            )
        )

    destatis_zensus_population_per_ha_inside_germany = gpd.read_postgis(
        cells_query.statement, cells_query.session.bind, index_col="id"
    )

    # add geom data of zensus cell
    missing_buildings_geom = pd.merge(
        left=destatis_zensus_population_per_ha_inside_germany[
            ["geom", "grid_id"]
        ],
        right=missing_buildings,
        left_index=True,
        right_index=True,
        how="right",
    )

    missing_buildings_geom = missing_buildings_geom.reset_index(drop=False)
    missing_buildings_geom = missing_buildings_geom.rename(
        columns={
            "building_count": "building_id",
            "cell_profile_ids": "profiles",
            "index": "cell_id",
        }
    )

    # cell bounds - half edge_length to not build buildings on the cell border
    xmin = missing_buildings_geom["geom"].bounds["minx"] + edge_length / 2
    xmax = missing_buildings_geom["geom"].bounds["maxx"] - edge_length / 2
    ymin = missing_buildings_geom["geom"].bounds["miny"] + edge_length / 2
    ymax = missing_buildings_geom["geom"].bounds["maxy"] - edge_length / 2

    # generate random coordinates within bounds - half edge_length
    np.random.seed(RANDOM_SEED)
    x = (xmax - xmin) * np.random.rand(missing_buildings_geom.shape[0]) + xmin
    y = (ymax - ymin) * np.random.rand(missing_buildings_geom.shape[0]) + ymin

    points = pd.Series([Point(cords) for cords in zip(x, y)])
    points = gpd.GeoSeries(points, crs="epsg:3035")
    # Buffer the points using a square cap style
    # Note cap_style: round = 1, flat = 2, square = 3
    buffer = points.buffer(edge_length / 2, cap_style=3)

    # store center of polygon
    missing_buildings_geom["geom_point"] = points
    # replace cell geom with new building geom
    missing_buildings_geom["geom"] = buffer
    missing_buildings_geom["osm_id"] = missing_buildings_geom["grid_id"]

    missing_buildings_geom["building_id"] += 1
    missing_buildings_geom["osm_id"] = (
        missing_buildings_geom["grid_id"]
        + "_"
        + missing_buildings_geom["building_id"].astype(str)
    )
    missing_buildings_geom = missing_buildings_geom.drop(
        columns=["building_id", "profiles"]
    )
    missing_buildings_geom["building"] = "residential"
    missing_buildings_geom["area"] = missing_buildings_geom["geom"].area

    return missing_buildings_geom


def generate_mapping_table(
    egon_map_zensus_buildings_filtered_synth,
    egon_household_electricity_profile_in_zensus_cell,
):
    """
    Generate a mapping table for hh profiles to buildings.

    All hh demand profiles are randomly assigned to buildings within the same
    cencus cell.

    * profiles > buildings: buildings have multiple profiles
    * profiles < buildings: not every building gets a profile


    Parameters
    ----------
    egon_map_zensus_buildings_filtered_synth: pd.DataFrame
        Table with OSM and synthetic buildings ids per census cell
    egon_household_electricity_profile_in_zensus_cell: pd.DataFrame
        Table mapping hh demand profiles to census cells

    Returns
    -------
    pd.DataFrame
        Table with mapping of profile ids to buildings with OSM ids

    """

    # group oms_ids by census cells and aggregate to list
    osm_ids_per_cell = (
        egon_map_zensus_buildings_filtered_synth[["osm_id", "cell_id"]]
        .groupby("cell_id")
        .agg(list)
    )

    # cell ids of cells with osm ids
    cells_with_buildings = osm_ids_per_cell.index.astype(int).values
    # cell ids of cells with profiles
    cells_with_profiles = (
        egon_household_electricity_profile_in_zensus_cell["cell_id"]
        .astype(int)
        .values
    )
    # cell ids of cells with osm ids and profiles
    cell_with_profiles_and_buildings = np.intersect1d(
        cells_with_profiles, cells_with_buildings
    )

    # cells_with_only_buildings = np.setdiff1d(cells_with_buildings, cells_with_profiles)
    # cells with only buildings might not be residential etc.

    # reduced list of profile_ids per cell with both buildings and profiles
    # should be same like egon_household_electricity_profile_in_zensus_cell.set_index('cell_id')['cell_profile_ids' ]
    profile_ids_per_cell_reduced = (
        egon_household_electricity_profile_in_zensus_cell.set_index(
            "cell_id"
        ).loc[cell_with_profiles_and_buildings, "cell_profile_ids"]
    )
    # reduced list of osm_ids per cell with both buildings and profiles
    osm_ids_per_cell_reduced = osm_ids_per_cell.loc[
        cell_with_profiles_and_buildings, "osm_id"
    ].rename("cell_osm_ids")

    # concat both lists by same cell_id
    mapping_profiles_to_buildings_reduced = pd.concat(
        [profile_ids_per_cell_reduced, osm_ids_per_cell_reduced], axis=1
    )

    # count number of profiles and buildings for each cell
    # tells how many profiles have to be assigned to how many buildings
    number_profiles_and_buildings_reduced = (
        mapping_profiles_to_buildings_reduced.applymap(len)
    )

    # map profiles randomly per cell
    rng = np.random.default_rng(RANDOM_SEED)
    mapping_profiles_to_buildings = pd.Series(
        [
            rng.integers(0, buildings, profiles)
            for buildings, profiles in zip(
                number_profiles_and_buildings_reduced["cell_osm_ids"].values,
                number_profiles_and_buildings_reduced[
                    "cell_profile_ids"
                ].values,
            )
        ],
        index=number_profiles_and_buildings_reduced.index,
    )
    # unnest building assignement per cell
    mapping_profiles_to_buildings = (
        mapping_profiles_to_buildings.rename("building")
        .explode()
        .reset_index()
    )
    # add profile position as attribute by number of entries per cell (*)
    mapping_profiles_to_buildings[
        "profile"
    ] = mapping_profiles_to_buildings.groupby(["cell_id"]).cumcount()
    # get multiindex of profiles in cells (*)
    index_profiles = mapping_profiles_to_buildings.set_index(
        ["cell_id", "profile"]
    ).index
    # get multiindex of buildings in cells (*)
    index_buildings = mapping_profiles_to_buildings.set_index(
        ["cell_id", "building"]
    ).index

    # get list of profiles by cell and profile position
    profile_ids_per_cell_reduced = (
        profile_ids_per_cell_reduced.explode().reset_index()
    )
    # assign profile position by order of list
    profile_ids_per_cell_reduced[
        "profile"
    ] = profile_ids_per_cell_reduced.groupby(["cell_id"]).cumcount()
    profile_ids_per_cell_reduced = profile_ids_per_cell_reduced.set_index(
        ["cell_id", "profile"]
    )

    # get list of building by cell and building number
    osm_ids_per_cell_reduced = osm_ids_per_cell_reduced.explode().reset_index()
    # assign building number by order of list
    osm_ids_per_cell_reduced["building"] = osm_ids_per_cell_reduced.groupby(
        ["cell_id"]
    ).cumcount()
    osm_ids_per_cell_reduced = osm_ids_per_cell_reduced.set_index(
        ["cell_id", "building"]
    )

    # map profiles and buildings by profile position and building number
    # merge is possible as both index results from the same origin (*) and are not rearranged
    mapping_profiles_to_buildings = pd.merge(
        osm_ids_per_cell_reduced.loc[index_buildings].reset_index(drop=False),
        profile_ids_per_cell_reduced.loc[index_profiles].reset_index(
            drop=True
        ),
        left_index=True,
        right_index=True,
    )

    return mapping_profiles_to_buildings


def map_houseprofiles_to_buildings():
    """
    Cencus hh demand profiles are assigned to buildings via osm ids. If no OSM
    ids available, synthetic buildings are generated. A list of the generated
    buildings and supplementary data as well as the mapping table is stored
    in the db.

    Tables:

    synthetic_buildings:
        schema: openstreetmap
        tablename: osm_buildings_synthetic

    mapping_profiles_to_buildings:
        schema: demand
        tablename: egon_household_electricity_profile_of_buildings

    Notes
    -----
    """
    #
    egon_map_zensus_buildings_filtered = Table(
        "egon_map_zensus_buildings_filtered",
        Base.metadata,
        schema="boundaries",
    )
    # get table metadata from db by name and schema
    inspect(engine).reflecttable(egon_map_zensus_buildings_filtered, None)

    with db.session_scope() as session:
        cells_query = session.query(egon_map_zensus_buildings_filtered)
    egon_map_zensus_buildings_filtered = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col=None
    )

    with db.session_scope() as session:
        cells_query = session.query(HouseholdElectricityProfilesInCensusCells)
    egon_household_electricity_profile_in_zensus_cell = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col=None
    )  # index_col="cell_id")

    # Match OSM and zensus data to define missing buildings
    missing_buildings = match_osm_and_zensus_data(
        egon_household_electricity_profile_in_zensus_cell,
        egon_map_zensus_buildings_filtered,
    )

    # randomly generate synthetic buildings in cell without any
    synthetic_buildings = generate_synthetic_buildings(missing_buildings,
                                                       edge_length=5)

    OsmBuildingsSynthetic.__table__.drop(bind=engine, checkfirst=True)
    OsmBuildingsSynthetic.__table__.create(bind=engine, checkfirst=True)

    # Write new buildings incl coord into db
    synthetic_buildings.to_postgis(
        "osm_buildings_synthetic",
        con=engine,
        if_exists="append",
        schema="openstreetmap",
        dtype={
            "osm_id": OsmBuildingsSynthetic.osm_id.type,
            "building": OsmBuildingsSynthetic.building.type,
            "cell_id": OsmBuildingsSynthetic.cell_id.type,
            "grid_id": OsmBuildingsSynthetic.grid_id.type,
            "geom": OsmBuildingsSynthetic.geom.type,
            "geom_point": OsmBuildingsSynthetic.geom_point.type,
            "area": OsmBuildingsSynthetic.area.type,
        },
    )

    # add synthetic buildings to df
    egon_map_zensus_buildings_filtered_synth = pd.concat(
        [
            egon_map_zensus_buildings_filtered,
            synthetic_buildings[["osm_id", "grid_id", "cell_id"]],
        ]
    )

    # assign profiles to buildings
    mapping_profiles_to_buildings = generate_mapping_table(
        egon_map_zensus_buildings_filtered_synth,
        egon_household_electricity_profile_in_zensus_cell,
    )

    HouseholdElectricityProfilesOfBuildings.__table__.drop(
        bind=engine, checkfirst=True
    )
    HouseholdElectricityProfilesOfBuildings.__table__.create(
        bind=engine, checkfirst=True
    )

    # Write building mapping into db
    with db.session_scope() as session:
        session.bulk_insert_mappings(
            HouseholdElectricityProfilesOfBuildings,
            mapping_profiles_to_buildings.to_dict(orient="records"),
        )


setup = partial(
    Dataset,
    name="Demand_Building_Assignment",
    version="0.0.0",
    dependencies=[],
    tasks=(map_houseprofiles_to_buildings),
)

# if __name__ == "__main__":
#     map_houseprofiles_to_buildings()

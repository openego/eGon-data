from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape
from sqlalchemy import REAL, Column, Float, Integer, String, func
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import pandas as pd
import saio

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.electricity_demand import (
    EgonDemandRegioZensusElectricity,
)
from egon.data.datasets.electricity_demand.temporal import calc_load_curves_cts
from egon.data.datasets.electricity_demand_timeseries.hh_buildings import (
    OsmBuildingsSynthetic,
)
from egon.data.datasets.electricity_demand_timeseries.tools import (
    random_ints_until_sum,
    random_point_in_square,
    specific_int_until_sum,
    write_table_to_postgis,
    write_table_to_postgres,
)
from egon.data.datasets.zensus_mv_grid_districts import MapZensusGridDistricts
from egon.data.datasets.zensus_vg250 import DestatisZensusPopulationPerHa
import egon.data.config

engine = db.engine()
Base = declarative_base()

data_config = egon.data.config.datasets()
RANDOM_SEED = egon.data.config.settings()["egon-data"]["--random-seed"]

# import db tables
saio.register_schema("openstreetmap", engine=engine)
saio.register_schema("society", engine=engine)
saio.register_schema("demand", engine=engine)
saio.register_schema("boundaries", engine=engine)


class EgonCtsElectricityDemandBuildingShare(Base):
    __tablename__ = "egon_cts_electricity_demand_building_share"
    __table_args__ = {"schema": "demand"}

    id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    bus_id = Column(Integer, index=True)
    profile_share = Column(Float)


class CtsPeakLoads(Base):
    __tablename__ = "egon_cts_peak_loads"
    __table_args__ = {"schema": "demand"}

    id = Column(String, primary_key=True)
    cts_peak_load_in_w_2035 = Column(REAL)
    cts_peak_load_in_w_100RE = Column(REAL)


class CtsBuildings(Base):
    __tablename__ = "egon_cts_buildings"
    __table_args__ = {"schema": "openstreetmap"}

    id = Column(Integer, primary_key=True)
    zensus_population_id = Column(Integer, index=True)
    geom_building = Column(Geometry("Polygon", 3035))
    n_amenities_inside = Column(Integer)
    source = Column(String)


def amenities_without_buildings():
    """

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
    Building centers are placed randomly within census cells.
    The Number of buildings is derived from n_amenity_inside, the selected
    method and number of amenities per building.
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
    """"""

    from saio.openstreetmap import osm_amenities_in_buildings_filtered

    with db.session_scope() as session:
        cells_query = (
            session.query(osm_amenities_in_buildings_filtered)
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
    # amenities per building
    # if building covers multiple cells, it exists multiple times
    df_amenities_in_buildings[
        "n_amenities_inside"
    ] = df_amenities_in_buildings.groupby(["zensus_population_id", "id"])[
        "n_amenities_inside"
    ].transform(
        "sum"
    )
    # reduce to buildings
    df_buildings_with_amenities = df_amenities_in_buildings.drop_duplicates(
        ["id", "zensus_population_id"]
    )
    df_buildings_with_amenities = df_buildings_with_amenities.reset_index(
        drop=True
    )
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

    return df_buildings_with_amenities


# TODO maybe replace with tools.write_table_to_db
def write_synthetic_buildings_to_db(df_synthetic_buildings):
    """"""
    if "geom_point" not in df_synthetic_buildings.columns:
        df_synthetic_buildings["geom_point"] = df_synthetic_buildings[
            "geom_building"
        ].centroid

    df_synthetic_buildings = df_synthetic_buildings.rename(
        columns={
            "zensus_population_id": "cell_id",
            "egon_building_id": "id",
        }
    )
    # Only take existing columns
    columns = [
        column.key for column in OsmBuildingsSynthetic.__table__.columns
    ]
    df_synthetic_buildings = df_synthetic_buildings.loc[:, columns]

    dtypes = {
        i: OsmBuildingsSynthetic.__table__.columns[i].type
        for i in OsmBuildingsSynthetic.__table__.columns.keys()
    }

    # Write new buildings incl coord into db
    df_synthetic_buildings.to_postgis(
        name=OsmBuildingsSynthetic.__tablename__,
        con=engine,
        if_exists="append",
        schema=OsmBuildingsSynthetic.__table_args__["schema"],
        dtype=dtypes,
    )


def buildings_without_amenities():
    """ """
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


def select_cts_buildings(df_buildings_wo_amenities):
    """ """
    # TODO Adapt method
    # Select one building each cell
    # take the first
    df_buildings_with_cts_demand = df_buildings_wo_amenities.drop_duplicates(
        # subset="cell_id", keep="first"
        subset="zensus_population_id",
        keep="first",
    ).reset_index(drop=True)
    df_buildings_with_cts_demand["n_amenities_inside"] = 1
    df_buildings_with_cts_demand["building"] = "cts"

    return df_buildings_with_cts_demand


def cells_with_cts_demand_only(df_buildings_without_amenities):
    """"""
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


def calc_census_cell_share(scenario="eGon2035"):
    """"""

    with db.session_scope() as session:
        cells_query = (
            session.query(
                EgonDemandRegioZensusElectricity, MapZensusGridDistricts.bus_id
            )
            .filter(EgonDemandRegioZensusElectricity.sector == "service")
            .filter(EgonDemandRegioZensusElectricity.scenario == scenario)
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
    df_census_share = df_census_share.rename("cell_share")

    df_census_share = pd.concat(
        [
            df_census_share,
            df_demand_regio_electricity_demand[["bus_id", "scenario"]],
        ],
        axis=1,
    )

    df_census_share.reset_index(inplace=True)
    return df_census_share


def calc_building_demand_profile_share(df_cts_buildings, scenario="eGon2035"):
    """
    Share of cts electricity demand profile per bus for every selected building
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

    df_census_cell_share = calc_census_cell_share(scenario)

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
    # Group and aggregate per building for multi cell buildings
    df_demand_share = (
        df_demand_share.groupby(["scenario", "bus_id", "id"])
        .sum()
        .reset_index()
    )

    return df_demand_share


def calc_building_profiles(
    df_demand_share=None, egon_building_id=None, scenario="eGon2035"
):
    """"""

    if not isinstance(df_demand_share, pd.DataFrame):
        with db.session_scope() as session:
            cells_query = session.query(EgonCtsElectricityDemandBuildingShare)

        df_demand_share = pd.read_sql(
            cells_query.statement, cells_query.session.bind, index_col=None
        )

    df_cts_profiles = calc_load_curves_cts(scenario)

    # Only calculate selected building profile if egon_building_id is given
    if (
        isinstance(egon_building_id, int)
        and egon_building_id in df_demand_share["id"]
    ):
        df_demand_share = df_demand_share.loc[
            df_demand_share["id"] == egon_building_id
        ]

    df_building_profiles = pd.DataFrame()
    for bus_id, df in df_demand_share.groupby("bus_id"):
        shares = df.set_index("id", drop=True)["profile_share"]
        profile = df_cts_profiles.loc[:, bus_id]
        building_profiles = profile.apply(lambda x: x * shares)
        df_building_profiles = pd.concat(
            [df_building_profiles, building_profiles], axis=1
        )

    return df_building_profiles


def cts_to_buildings():
    """"""

    # #### NOTE
    # #### Cells with CTS demand, amenities and buildings do not change
    # #### within the scenarios, only the demand itself. Therefore
    # #### scenario eGon2035 can be used universally to determine
    # #### buildings

    # Buildings with amenities
    df_buildings_with_amenities = buildings_with_amenities()

    # Remove synthetic CTS buildings if existing
    delete_synthetic_cts_buildings()

    # Create synthetic buildings for amenites without buildings
    df_amenities_without_buildings = amenities_without_buildings()
    df_amenities_without_buildings["n_amenities_inside"] = 1
    df_synthetic_buildings_with_amenities = create_synthetic_buildings(
        df_amenities_without_buildings, points="geom_amenity"
    )

    # TODO write to DB and remove renaming
    # write_synthetic_buildings_to_db(df_synthetic_buildings_with_amenities)
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

    # Cells without amenities but CTS demand and buildings
    df_buildings_without_amenities = buildings_without_amenities()

    # TODO Fix Adhoc Bugfix duplicated buildings
    mask = df_buildings_without_amenities.loc[
        df_buildings_without_amenities["id"].isin(
            df_buildings_with_amenities["id"]
        )
    ].index
    df_buildings_without_amenities = df_buildings_without_amenities.drop(
        index=mask
    ).reset_index(drop=True)

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
    df_synthetic_buildings_without_amenities = create_synthetic_buildings(
        df_cells_with_cts_demand_only, points="geom_point"
    )

    # TODO write to DB and remove renaming
    # write_synthetic_buildings_to_db(df_synthetic_buildings_without_amenities)
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
    write_table_to_postgis(
        df_cts_buildings,
        CtsBuildings,
        drop=True,
    )

    df_demand_share_2035 = calc_building_demand_profile_share(
        df_cts_buildings, scenario="eGon2035"
    )
    df_demand_share_100RE = calc_building_demand_profile_share(
        df_cts_buildings, scenario="eGon100RE"
    )

    df_demand_share = pd.concat(
        [df_demand_share_2035, df_demand_share_100RE],
        axis=0,
        ignore_index=True,
    )

    # TODO Why are there nonunique ids?
    #  needs to be removed as soon as 'id' is unique
    df_demand_share = df_demand_share.drop_duplicates(subset="id")

    write_table_to_postgres(
        df_demand_share, EgonCtsElectricityDemandBuildingShare, drop=True
    )

    return df_cts_buildings, df_demand_share


def get_peak_load_cts_buildings():

    # TODO Check units, maybe MwH?
    df_building_profiles = calc_building_profiles(scenario="eGon2035")
    df_peak_load_2035 = df_building_profiles.max(axis=0).rename(
        "cts_peak_load_in_w_2035"
    )
    df_building_profiles = calc_building_profiles(scenario="eGon2035")
    df_peak_load_100RE = df_building_profiles.max(axis=0).rename(
        "cts_peak_load_in_w_100RE"
    )
    df_peak_load = pd.concat(
        [df_peak_load_2035, df_peak_load_100RE], axis=1
    ).reset_index()

    CtsPeakLoads.__table__.drop(bind=engine, checkfirst=True)
    CtsPeakLoads.__table__.create(bind=engine, checkfirst=True)

    # Write peak loads into db
    with db.session_scope() as session:
        session.bulk_insert_mappings(
            CtsPeakLoads,
            df_peak_load.to_dict(orient="records"),
        )


def delete_synthetic_cts_buildings():
    # import db tables
    from saio.openstreetmap import osm_buildings_synthetic

    # cells mit amenities
    with db.session_scope() as session:
        session.query(osm_buildings_synthetic).filter(
            osm_buildings_synthetic.building == "cts"
        ).delete()


class CtsElectricityBuildings(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="CtsElectricityBuildings",
            version="0.0.0.",
            dependencies=dependencies,
            tasks=(
                cts_to_buildings,
                get_peak_load_cts_buildings,
                # get_all_cts_building_profiles,
            ),
        )

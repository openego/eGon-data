"""
CTS electricity and heat demand time series for scenarios in 2035 and 2050
assigned to OSM-buildings are generated.

Disaggregation of CTS heat & electricity demand time series from MV substation
to census cells via annual demand and then to OSM buildings via
amenity tags or randomly if no sufficient OSM-data is available in the
respective census cell. If no OSM-buildings or synthetic residential buildings
are available new synthetic 5x5m buildings are generated.
"""

from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy import REAL, Column, Integer, String, func
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd
import saio

from egon.data import db
from egon.data import logger as log
from egon.data.datasets import Dataset
from egon.data.datasets.electricity_demand import (
    EgonDemandRegioZensusElectricity,
)
from egon.data.datasets.electricity_demand.temporal import (
    EgonEtragoElectricityCts,
)
from egon.data.datasets.electricity_demand_timeseries.hh_buildings import (
    BuildingElectricityPeakLoads,
    OsmBuildingsSynthetic,
)
from egon.data.datasets.electricity_demand_timeseries.mapping import (
    map_all_used_buildings,
)
from egon.data.datasets.electricity_demand_timeseries.tools import (
    random_ints_until_sum,
    random_point_in_square,
    specific_int_until_sum,
    write_table_to_postgis,
    write_table_to_postgres,
)
from egon.data.datasets.heat_demand import EgonPetaHeat
from egon.data.datasets.heat_demand_timeseries import EgonEtragoHeatCts
from egon.data.datasets.zensus_mv_grid_districts import MapZensusGridDistricts
from egon.data.datasets.zensus_vg250 import DestatisZensusPopulationPerHa

engine = db.engine()
Base = declarative_base()

# import db tables
saio.register_schema("openstreetmap", engine=engine)
saio.register_schema("boundaries", engine=engine)


class EgonCtsElectricityDemandBuildingShare(Base):
    """
    Class definition of table demand.egon_cts_electricity_demand_building_share.

    Table including the MV substation electricity profile share of all selected
    CTS buildings for scenario eGon2035 and eGon100RE. This table is created
    within :func:`cts_electricity()`.
    """
    __tablename__ = "egon_cts_electricity_demand_building_share"
    __table_args__ = {"schema": "demand"}

    building_id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    bus_id = Column(Integer, index=True)
    profile_share = Column(REAL)


class EgonCtsHeatDemandBuildingShare(Base):
    """
    Class definition of table demand.egon_cts_heat_demand_building_share.

    Table including the MV substation heat profile share of all selected
    CTS buildings for scenario eGon2035 and eGon100RE. This table is created
    within :func:`cts_heat()`.
    """
    __tablename__ = "egon_cts_heat_demand_building_share"
    __table_args__ = {"schema": "demand"}

    building_id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    bus_id = Column(Integer, index=True)
    profile_share = Column(REAL)


class CtsBuildings(Base):
    """
    Class definition of table openstreetmap.egon_cts_buildings.

    Table of all selected CTS buildings with id, census cell id, geometry and
    amenity count in building. This table is created within
    :func:`cts_buildings()`.
    """
    __tablename__ = "egon_cts_buildings"
    __table_args__ = {"schema": "openstreetmap"}

    serial = Column(Integer, primary_key=True)
    id = Column(Integer, index=True)
    zensus_population_id = Column(Integer, index=True)
    geom_building = Column(Geometry("Polygon", 3035))
    n_amenities_inside = Column(Integer)
    source = Column(String)


class BuildingHeatPeakLoads(Base):
    """
    Class definition of table demand.egon_building_heat_peak_loads.

    """
    __tablename__ = "egon_building_heat_peak_loads"
    __table_args__ = {"schema": "demand"}

    building_id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    sector = Column(String, primary_key=True)
    peak_load_in_w = Column(REAL)


class CtsDemandBuildings(Dataset):
    """
    Generates CTS electricity and heat demand time series for scenarios in 2035 and 2050
    assigned to OSM-buildings.

    Disaggregation of CTS heat & electricity demand time series from MV Substation
    to census cells via annual demand and then to OSM buildings via
    amenity tags or randomly if no sufficient OSM-data is available in the
    respective census cell. If no OSM-buildings or synthetic residential buildings
    are available new synthetic 5x5m buildings are generated.


    *Dependencies*
      * :py:class:`OsmBuildingsStreets <egon.data.datasets.osm_buildings_streets.OsmBuildingsStreets>`
      * :py:class:`CtsElectricityDemand <egon.data.datasets.electricity_demand.CtsElectricityDemand>`
      * :py:class:`hh_buildings <egon.data.datasets.electricity_demand_timeseries.hh_buildings>`
      * :py:class:`HeatTimeSeries <egon.data.datasets.heat_demand_timeseries.HeatTimeSeries>` (more specifically the :func:`export_etrago_cts_heat_profiles <egon.data.datasets.heat_demand_timeseries.export_etrago_cts_heat_profiles>` task)

    *Resulting tables*
      * :py:class:`openstreetmap.osm_buildings_synthetic <egon.data.datasets.electricity_demand_timeseries.hh_buildings.OsmBuildingsSynthetic>` is extended
      * :py:class:`openstreetmap.egon_cts_buildings <egon.data.datasets.electricity_demand_timeseries.cts_buildings.CtsBuildings> is created
      * :py:class:`demand.egon_cts_electricity_demand_building_share <egon.data.datasets.electricity_demand_timeseries.cts_buildings.EgonCtsElectricityDemandBuildingShare>` is created
      * :py:class:`demand.egon_cts_heat_demand_building_share <egon.data.datasets.electricity_demand_timeseries.cts_buildings.EgonCtsHeatDemandBuildingShare>` is created
      * :py:class:`demand.egon_building_electricity_peak_loads <egon.data.datasets.electricity_demand_timeseries.hh_buildings.BuildingElectricityPeakLoads>` is extended
      * :py:class:`boundaries.egon_map_zensus_mvgd_buildings <egon.data.datasets.electricity_demand_timeseries.mapping.EgonMapZensusMvgdBuildings>` is extended.


    **The following datasets from the database are mainly used for creation:**

    * `openstreetmap.osm_buildings_filtered`:
        Table of OSM-buildings filtered by tags to selecting residential and cts
        buildings only.
    * `openstreetmap.osm_amenities_shops_filtered`:
        Table of OSM-amenities filtered by tags to select cts only.
    * `openstreetmap.osm_amenities_not_in_buildings_filtered`:
        Table of amenities which do not intersect with any building from
        `openstreetmap.osm_buildings_filtered`
    * `openstreetmap.osm_buildings_synthetic`:
        Table of synthetic residential buildings
    * `boundaries.egon_map_zensus_buildings_filtered_all`:
        Mapping table of census cells and buildings filtered even if population
        in census cell = 0.
    * `demand.egon_demandregio_zensus_electricity`:
        Table of annual electricity load demand for residential and cts at census
        cell level. Residential load demand is derived from aggregated residential
        building profiles. DemandRegio CTS load demand at NUTS3 is distributed to
        census cells linearly to heat demand from peta5.
    * `demand.egon_peta_heat`:
        Table of annual heat load demand for residential and cts at census cell
        level from peta5.
    * `demand.egon_etrago_electricity_cts`:
        Scaled cts electricity time series for every MV substation. Derived from
        DemandRegio SLP for selected economic sectors at nuts3. Scaled with annual
        demand from `demand.egon_demandregio_zensus_electricity`
    * `demand.egon_etrago_heat_cts`:
        Scaled cts heat time series for every MV substation. Derived from
        DemandRegio SLP Gas for selected economic sectors at nuts3. Scaled with
        annual demand from `demand.egon_peta_heat`.

    **What is the goal?**

    To disaggregate cts heat and electricity time series from MV substation level
    to geo-referenced buildings, the annual demand from DemandRegio and Peta5 is
    used to identify census cells with load demand. We use Openstreetmap data and
    filter tags to identify buildings and count the  amenities within. The number
    of amenities and the annual demand serve to assign a demand share of the MV
    substation profile to the building.

    **What is the challenge?**

    The OSM, DemandRegio and Peta5 dataset differ from each other. The OSM dataset
    is a community based dataset which is extended throughout and does not claim to
    be complete. Therefore, not all census cells which have a demand assigned by
    DemandRegio or Peta5 methodology also have buildings with respective tags or
    sometimes even any building at all. Furthermore, the substation load areas are
    determined dynamically in a previous dataset. Merging these datasets different
    scopes (census cell shapes, building shapes) and their inconsistencies need to
    be addressed. For example: not yet tagged buildings or amenities in OSM, or
    building shapes exceeding census cells.


    **How are these datasets combined?**


    The methodology for heat and electricity is the same and only differs in the
    annual demand and MV/HV Substation profile. In a previous dataset
    (openstreetmap), we filter all OSM buildings and amenities for tags, we relate
    to the cts sector. Amenities are mapped to intersecting buildings and then
    intersected with the annual demand which exists at census cell level. We obtain
    census cells with demand and amenities and without amenities. If there is no
    data on amenities, n synthetic ones are assigned to existing buildings. We use
    the median value of amenities/census cell for n and all filtered buildings +
    synthetic residential buildings. If no building data is available a synthetic
    buildings is randomly generated. This also happens for amenities which couldn't
    be assigned to any osm building. All census cells with an annual demand are
    covered this way, and we obtain four different categories of buildings with
    amenities:

    * Buildings with amenities
    * Synthetic buildings with amenities
    * Buildings with synthetic amenities
    * Synthetics buildings with synthetic amenities

    The amenities are summed per census cell (of amenity) and building to derive
    the building amenity share per census cell. Multiplied with the annual demand,
    we receive the profile demand share for each cell. Some buildings exceed the
    census cell shape and have amenities in different cells although mapped to one
    building only. To have unique buildings the demand share is summed once more
    per building id. This factor can now be used to obtain the profile for each
    building.

    A schematic flow chart exist in the correspondent issue #671:
    https://github.com/openego/eGon-data/issues/671#issuecomment-1260740258


    **What are central assumptions during the data processing?**

    * We assume OSM data to be the most reliable and complete open source dataset.
    * We assume building and amenity tags to be truthful and accurate.
    * Mapping census to OSM data is not trivial. Discrepancies are substituted.
    * Missing OSM buildings are generated for each amenity.
    * Missing amenities are generated by median value of amenities/census cell.


    **Drawbacks and limitations of the data**

    * Shape of profiles for each building is similar within a MVGD and only scaled
      with a different factor.
    * MVGDs are generated dynamically. In case of buildings with amenities
      exceeding MVGD borders, amenities which are assigned to a different MVGD than
      the assigned building centroid, the amenities are dropped for sake of
      simplicity. One building should not have a connection to two MVGDs.
    * The completeness of the OSM data depends on community contribution and is
      crucial to the quality of our results.
    * Randomly selected buildings and generated amenities may inadequately reflect
      reality, but are chosen for sake of simplicity as a measure to fill data gaps.
    * Since this dataset is a cascade after generation of synthetic residential
      buildings also check drawbacks and limitations in hh_buildings.py.
    * Synthetic buildings may be placed within osm buildings which exceed multiple
      census cells. This is currently accepted but may be solved in #953.
    * Scattered high peak loads occur and might lead to single MV grid connections
      in ding0. In some cases this might not be viable. Postprocessing is needed and
      may be solved in #954.

    """

    #:
    name: str = "CtsDemandBuildings"
    #:
    version: str = "0.0.3"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                cts_buildings,
                {cts_electricity, cts_heat},
                get_cts_electricity_peak_load,
                map_all_used_buildings,
                assign_voltage_level_to_buildings,
            ),
        )


def amenities_without_buildings():
    """
    Amenities which have no buildings assigned and are in a cell with cts
    demand are determined.

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
                osm_amenities_not_in_buildings_filtered.geom_amenity,
                osm_amenities_not_in_buildings_filtered.egon_amenity_id,
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
                EgonDemandRegioZensusElectricity.scenario == "eGon2035",
            )
        )

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

    df = gpd.GeoDataFrame(
        df,
        crs=crs,
        geometry="geom_building",
    )

    # TODO remove after #772 implementation of egon_building_id
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
    # TODO remove after #772
    df = df.rename(
        columns={
            # "zensus_population_id": "cell_id",
            "egon_building_id": "id",
        }
    )
    return df


def buildings_with_amenities():
    """
    Amenities which are assigned to buildings are determined and grouped per
    building and zensus cell. Buildings covering multiple cells therefore
    exists multiple times but in different zensus cells. This is necessary to
    cover as many cells with a cts demand as possible. If buildings exist in
    multiple mvgds (bus_id) , only the amenities within the same as the
    building centroid are kept. If as a result, a census cell is uncovered
    by any buildings, a synthetic amenity is placed. The buildings are
    aggregated afterwards during the calculation of the profile_share.

    Returns
    -------
    df_buildings_with_amenities: gpd.GeoDataFrame
        Contains all buildings with amenities per zensus cell.
    df_lost_cells: gpd.GeoDataFrame
        Contains synthetic amenities in lost cells. Might be empty
    """

    from saio.boundaries import egon_map_zensus_buildings_filtered_all
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
            cells_query.statement, con=session.connection(), index_col=None
        )

    df_amenities_in_buildings["geom_building"] = df_amenities_in_buildings[
        "geom_building"
    ].apply(to_shape)
    df_amenities_in_buildings["geom_amenity"] = df_amenities_in_buildings[
        "geom_amenity"
    ].apply(to_shape)

    # retrieve building centroid bus_id
    with db.session_scope() as session:

        cells_query = session.query(
            egon_map_zensus_buildings_filtered_all.id,
            MapZensusGridDistricts.bus_id.label("building_bus_id"),
        ).filter(
            egon_map_zensus_buildings_filtered_all.zensus_population_id
            == MapZensusGridDistricts.zensus_population_id
        )

        df_building_bus_id = pd.read_sql(
            cells_query.statement, con=session.connection(), index_col=None
        )

    df_amenities_in_buildings = pd.merge(
        left=df_amenities_in_buildings, right=df_building_bus_id, on="id"
    )

    # identify amenities with differing bus_id as building
    identified_amenities = df_amenities_in_buildings.loc[
        df_amenities_in_buildings["bus_id"]
        != df_amenities_in_buildings["building_bus_id"]
    ].index

    lost_cells = df_amenities_in_buildings.loc[
        identified_amenities, "zensus_population_id"
    ].unique()

    # check if lost zensus cells are already covered
    if not (
        df_amenities_in_buildings["zensus_population_id"]
        .isin(lost_cells)
        .empty
    ):
        # query geom data for cell if not
        with db.session_scope() as session:
            cells_query = session.query(
                DestatisZensusPopulationPerHa.id,
                DestatisZensusPopulationPerHa.geom,
            ).filter(
                DestatisZensusPopulationPerHa.id.in_(pd.Index(lost_cells))
            )

            df_lost_cells = gpd.read_postgis(
                cells_query.statement,
                cells_query.session.bind,
                geom_col="geom",
            )

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

    df_amenities_in_buildings.drop(identified_amenities, inplace=True)
    df_amenities_in_buildings.drop(columns="building_bus_id", inplace=True)

    df_amenities_in_buildings["n_amenities_inside"] = 1

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

    # TODO remove after #722
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


def calc_census_cell_share(scenario, sector):
    """
    The profile share for each census cell is calculated by it's
    share of annual demand per substation bus. The annual demand
    per cell is defined by DemandRegio/Peta5. The share is for both
    scenarios identical as the annual demand is linearly scaled.

    Parameters
    ----------
    scenario: str
        Scenario for which the share is calculated: "eGon2035" or "eGon100RE"
    sector: str
        Scenario for which the share is calculated: "electricity" or "heat"

    Returns
    -------
    df_census_share: pd.DataFrame
    """
    if sector == "electricity":
        with db.session_scope() as session:
            cells_query = (
                session.query(
                    EgonDemandRegioZensusElectricity,
                    MapZensusGridDistricts.bus_id,
                )
                .filter(EgonDemandRegioZensusElectricity.sector == "service")
                .filter(EgonDemandRegioZensusElectricity.scenario == scenario)
                .filter(
                    EgonDemandRegioZensusElectricity.zensus_population_id
                    == MapZensusGridDistricts.zensus_population_id
                )
            )

    elif sector == "heat":
        with db.session_scope() as session:
            cells_query = (
                session.query(EgonPetaHeat, MapZensusGridDistricts.bus_id)
                .filter(EgonPetaHeat.sector == "service")
                .filter(EgonPetaHeat.scenario == scenario)
                .filter(
                    EgonPetaHeat.zensus_population_id
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
    grouped and aggregated per building as some buildings exceed the shape of
    census cells and have amenities assigned from multiple cells. Building
    therefore get the amenity share of all census cells.

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
        within a census cell. Building ids can exist multiple time but with
        different zensus_population_ids.
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

    # only pass selected columns
    df_demand_share = df_demand_share[
        ["id", "bus_id", "scenario", "profile_share"]
    ]

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


def get_peta_demand(mvgd, scenario):
    """
    Retrieve annual peta heat demand for CTS for either
    eGon2035 or eGon100RE scenario.

    Parameters
    ----------
    mvgd : int
        ID of substation for which to get CTS demand.
    scenario : str
        Possible options are eGon2035 or eGon100RE

    Returns
    -------
    df_peta_demand : pd.DataFrame
        Annual residential heat demand per building and scenario. Columns of
        the dataframe are zensus_population_id and demand.

    """

    with db.session_scope() as session:
        query = (
            session.query(
                MapZensusGridDistricts.zensus_population_id,
                EgonPetaHeat.demand,
            )
            .filter(MapZensusGridDistricts.bus_id == int(mvgd))
            .filter(
                MapZensusGridDistricts.zensus_population_id
                == EgonPetaHeat.zensus_population_id
            )
            .filter(
                EgonPetaHeat.sector == "service",
                EgonPetaHeat.scenario == scenario,
            )
        )

        df_peta_demand = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )

    return df_peta_demand


def calc_cts_building_profiles(
    bus_ids,
    scenario,
    sector,
):
    """
    Calculate the cts demand profile for each building. The profile is
    calculated by the demand share of the building per substation bus.

    Parameters
    ----------
    bus_ids: list of int
        Ids of the substation for which selected building profiles are
        calculated.
    scenario: str
        Scenario for which the share is calculated: "eGon2035" or "eGon100RE"
    sector: str
        Sector for which the share is calculated: "electricity" or "heat"

    Returns
    -------
    df_building_profiles: pd.DataFrame
        Table of demand profile per building. Column names are building IDs
        and index is hour of the year as int (0-8759).

    """
    if sector == "electricity":
        # Get cts building electricity demand share of selected buildings
        with db.session_scope() as session:
            cells_query = (
                session.query(
                    EgonCtsElectricityDemandBuildingShare,
                )
                .filter(
                    EgonCtsElectricityDemandBuildingShare.scenario == scenario
                )
                .filter(
                    EgonCtsElectricityDemandBuildingShare.bus_id.in_(bus_ids)
                )
            )

        df_demand_share = pd.read_sql(
            cells_query.statement, cells_query.session.bind, index_col=None
        )

        # Get substation cts electricity load profiles of selected bus_ids
        with db.session_scope() as session:
            cells_query = (
                session.query(EgonEtragoElectricityCts).filter(
                    EgonEtragoElectricityCts.scn_name == scenario
                )
            ).filter(EgonEtragoElectricityCts.bus_id.in_(bus_ids))

        df_cts_substation_profiles = pd.read_sql(
            cells_query.statement,
            cells_query.session.bind,
        )
        df_cts_substation_profiles = pd.DataFrame.from_dict(
            df_cts_substation_profiles.set_index("bus_id")["p_set"].to_dict(),
            orient="index",
        )
        # df_cts_profiles = calc_load_curves_cts(scenario)

    elif sector == "heat":
        # Get cts building heat demand share of selected buildings
        with db.session_scope() as session:
            cells_query = (
                session.query(
                    EgonCtsHeatDemandBuildingShare,
                )
                .filter(EgonCtsHeatDemandBuildingShare.scenario == scenario)
                .filter(EgonCtsHeatDemandBuildingShare.bus_id.in_(bus_ids))
            )

        df_demand_share = pd.read_sql(
            cells_query.statement, cells_query.session.bind, index_col=None
        )

        # Get substation cts heat load profiles of selected bus_ids
        # (this profile only contains zensus cells with individual heating;
        #  in order to obtain a profile for the whole MV grid it is afterwards
        #  scaled by the grids total CTS demand from peta)
        with db.session_scope() as session:
            cells_query = (
                session.query(EgonEtragoHeatCts).filter(
                    EgonEtragoHeatCts.scn_name == scenario
                )
            ).filter(EgonEtragoHeatCts.bus_id.in_(bus_ids))

        df_cts_substation_profiles = pd.read_sql(
            cells_query.statement,
            cells_query.session.bind,
        )
        df_cts_substation_profiles = pd.DataFrame.from_dict(
            df_cts_substation_profiles.set_index("bus_id")["p_set"].to_dict(),
            orient="index",
        )
        for bus_id in bus_ids:
            if bus_id in df_cts_substation_profiles.index:
                # get peta demand to scale load profile to
                peta_cts_demand = get_peta_demand(bus_id, scenario)
                scaling_factor = (
                    peta_cts_demand.demand.sum()
                    / df_cts_substation_profiles.loc[bus_id, :].sum()
                )
                # scale load profile
                df_cts_substation_profiles.loc[bus_id, :] *= scaling_factor

    else:
        raise KeyError("Sector needs to be either 'electricity' or 'heat'")

    # TODO remove after #722
    df_demand_share.rename(columns={"id": "building_id"}, inplace=True)

    # get demand profile for all buildings for selected demand share
    df_building_profiles = pd.DataFrame()
    for bus_id, df in df_demand_share.groupby("bus_id"):
        shares = df.set_index("building_id", drop=True)["profile_share"]
        try:
            profile_ts = df_cts_substation_profiles.loc[bus_id]
        except KeyError:
            # This should only happen within the SH cutout
            log.info(
                f"No CTS profile found for substation with bus_id:"
                f" {bus_id}"
            )
            continue

        building_profiles = np.outer(profile_ts, shares)
        building_profiles = pd.DataFrame(
            building_profiles, index=profile_ts.index, columns=shares.index
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


def remove_double_bus_id(df_cts_buildings):
    """This is an backup adhoc fix if there should still be a building which
    is assigned to 2 substations. In this case one of the buildings is just
    dropped. As this currently accounts for only one building with one amenity
    the deviation is neglectable."""
    # assign bus_id via census cell of amenity
    with db.session_scope() as session:
        cells_query = session.query(
            MapZensusGridDistricts.zensus_population_id,
            MapZensusGridDistricts.bus_id,
        )

    df_egon_map_zensus_buildings_buses = pd.read_sql(
        cells_query.statement,
        cells_query.session.bind,
        index_col=None,
    )
    df_cts_buildings = pd.merge(
        left=df_cts_buildings,
        right=df_egon_map_zensus_buildings_buses,
        on="zensus_population_id",
    )

    substation_per_building = df_cts_buildings.groupby("id")[
        "bus_id"
    ].nunique()
    building_id = substation_per_building.loc[
        substation_per_building > 1
    ].index
    df_duplicates = df_cts_buildings.loc[
        df_cts_buildings["id"].isin(building_id)
    ]
    for unique_id in df_duplicates["id"].unique():
        drop_index = df_duplicates[df_duplicates["id"] == unique_id].index[0]
        print(
            f"Buildings {df_cts_buildings.loc[drop_index, 'id']}"
            f" dropped because of double substation"
        )
        df_cts_buildings.drop(index=drop_index, inplace=True)

    df_cts_buildings.drop(columns="bus_id", inplace=True)

    return df_cts_buildings


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
    the demand share.
    """
    # ========== Register np datatypes with SQLA ==========
    def adapt_numpy_float64(numpy_float64):
        return AsIs(numpy_float64)

    def adapt_numpy_int64(numpy_int64):
        return AsIs(numpy_int64)

    register_adapter(np.float64, adapt_numpy_float64)
    register_adapter(np.int64, adapt_numpy_int64)
    # =====================================================

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
    log.info(f"Median amenity value: {median_n_amenities}")

    # Remove synthetic CTS buildings if existing
    delete_synthetic_cts_buildings()
    log.info("Old synthetic cts buildings deleted!")

    # Amenities not assigned to buildings
    df_amenities_without_buildings = amenities_without_buildings()
    log.info("Amenities without buildlings selected!")

    # Append lost cells due to duplicated ids, to cover all demand cells
    if not df_lost_cells.empty:

        # Number of synth amenities per cell
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
        log.info(
            f"{df_lost_cells.shape[0]} lost cells due to substation "
            f"intersection appended!"
        )

    # One building per amenity
    df_amenities_without_buildings["n_amenities_inside"] = 1
    # Create synthetic buildings for amenites without buildings
    df_synthetic_buildings_with_amenities = create_synthetic_buildings(
        df_amenities_without_buildings, points="geom_amenity"
    )
    log.info("Synthetic buildings created!")

    # TODO remove renaming after #722
    write_table_to_postgis(
        df_synthetic_buildings_with_amenities.rename(
            columns={
                "zensus_population_id": "cell_id",
                "egon_building_id": "id",
            }
        ),
        OsmBuildingsSynthetic,
        engine=engine,
        drop=False,
    )
    log.info("Synthetic buildings exported to DB!")

    # Cells without amenities but CTS demand and buildings
    df_buildings_without_amenities = buildings_without_amenities()
    log.info("Buildings without amenities in demand cells identified!")

    # Backup Bugfix for duplicated buildings which occure in SQL-Querry
    # drop building ids which have already been used
    mask = df_buildings_without_amenities.loc[
        df_buildings_without_amenities["id"].isin(
            df_buildings_with_amenities["id"]
        )
    ].index
    df_buildings_without_amenities = df_buildings_without_amenities.drop(
        index=mask
    ).reset_index(drop=True)
    log.info(f"{len(mask)} duplicated ids removed!")

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

    # TODO implement overlay prevention #953 here
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

    # TODO remove renaming after #722
    write_table_to_postgis(
        df_synthetic_buildings_without_amenities.rename(
            columns={
                "zensus_population_id": "cell_id",
                "egon_building_id": "id",
            }
        ),
        OsmBuildingsSynthetic,
        engine=engine,
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
    df_cts_buildings = remove_double_bus_id(df_cts_buildings)
    log.info("Double bus_id checked")

    # TODO remove dypte correction after #722
    df_cts_buildings["id"] = df_cts_buildings["id"].astype(int)

    df_cts_buildings = gpd.GeoDataFrame(
        df_cts_buildings, geometry="geom_building", crs=3035
    )
    df_cts_buildings = df_cts_buildings.reset_index().rename(
        columns={"index": "serial"}
    )

    # Write table to db for debugging and postprocessing
    write_table_to_postgis(
        df_cts_buildings,
        CtsBuildings,
        engine=engine,
        drop=True,
    )
    log.info("CTS buildings exported to DB!")


def cts_electricity():
    """
    Calculate cts electricity demand share of hvmv substation profile
     for buildings.
    """
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
    df_demand_share.rename(columns={"id": "building_id"}, inplace=True)

    write_table_to_postgres(
        df_demand_share,
        EgonCtsElectricityDemandBuildingShare,
        drop=True,
    )
    log.info("Profile share exported to DB!")


def cts_heat():
    """
    Calculate cts electricity demand share of hvmv substation profile
     for buildings.
    """
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

    df_demand_share.rename(columns={"id": "building_id"}, inplace=True)

    write_table_to_postgres(
        df_demand_share,
        EgonCtsHeatDemandBuildingShare,
        drop=True,
    )
    log.info("Profile share exported to DB!")


def get_cts_electricity_peak_load():
    """
    Get electricity peak load of all CTS buildings for both scenarios and
    store in DB.
    """
    log.info("Start logging!")

    BuildingElectricityPeakLoads.__table__.create(bind=engine, checkfirst=True)

    # Delete rows with cts demand
    with db.session_scope() as session:
        session.query(BuildingElectricityPeakLoads).filter(
            BuildingElectricityPeakLoads.sector == "cts"
        ).delete()
    log.info("Cts electricity peak load removed from DB!")

    for scenario in ["eGon2035", "eGon100RE"]:

        with db.session_scope() as session:
            cells_query = session.query(
                EgonCtsElectricityDemandBuildingShare
            ).filter(
                EgonCtsElectricityDemandBuildingShare.scenario == scenario
            )

        df_demand_share = pd.read_sql(
            cells_query.statement, cells_query.session.bind, index_col=None
        )

        with db.session_scope() as session:
            cells_query = session.query(EgonEtragoElectricityCts).filter(
                EgonEtragoElectricityCts.scn_name == scenario
            )

        df_cts_profiles = pd.read_sql(
            cells_query.statement,
            cells_query.session.bind,
        )
        df_cts_profiles = pd.DataFrame.from_dict(
            df_cts_profiles.set_index("bus_id")["p_set"].to_dict(),
            orient="columns",
        )

        df_peak_load = pd.merge(
            left=df_cts_profiles.max().astype(float).rename("max"),
            right=df_demand_share,
            left_index=True,
            right_on="bus_id",
        )

        # Convert unit from MWh to W
        df_peak_load["max"] = df_peak_load["max"] * 1e6
        df_peak_load["peak_load_in_w"] = (
            df_peak_load["max"] * df_peak_load["profile_share"]
        )
        log.info(f"Peak load for {scenario} determined!")

        # TODO remove after #772
        df_peak_load.rename(columns={"id": "building_id"}, inplace=True)
        df_peak_load["sector"] = "cts"

        # # Write peak loads into db
        write_table_to_postgres(
            df_peak_load,
            BuildingElectricityPeakLoads,
            drop=False,
            index=False,
            if_exists="append",
        )

        log.info(f"Peak load for {scenario} exported to DB!")


def get_cts_heat_peak_load():
    """
    Get heat peak load of all CTS buildings for both scenarios and store in DB.
    """
    log.info("Start logging!")

    BuildingHeatPeakLoads.__table__.create(bind=engine, checkfirst=True)

    # Delete rows with cts demand
    with db.session_scope() as session:
        session.query(BuildingHeatPeakLoads).filter(
            BuildingHeatPeakLoads.sector == "cts"
        ).delete()
    log.info("Cts heat peak load removed from DB!")

    for scenario in ["eGon2035", "eGon100RE"]:

        with db.session_scope() as session:
            cells_query = session.query(
                EgonCtsElectricityDemandBuildingShare
            ).filter(
                EgonCtsElectricityDemandBuildingShare.scenario == scenario
            )

        df_demand_share = pd.read_sql(
            cells_query.statement, cells_query.session.bind, index_col=None
        )
        log.info(f"Retrieved demand share for scenario: {scenario}")

        with db.session_scope() as session:
            cells_query = session.query(EgonEtragoHeatCts).filter(
                EgonEtragoHeatCts.scn_name == scenario
            )

        df_cts_profiles = pd.read_sql(
            cells_query.statement,
            cells_query.session.bind,
        )
        log.info(f"Retrieved substation profiles for scenario: {scenario}")

        df_cts_profiles = pd.DataFrame.from_dict(
            df_cts_profiles.set_index("bus_id")["p_set"].to_dict(),
            orient="columns",
        )

        df_peak_load = pd.merge(
            left=df_cts_profiles.max().astype(float).rename("max"),
            right=df_demand_share,
            left_index=True,
            right_on="bus_id",
        )

        # Convert unit from MWh to W
        df_peak_load["max"] = df_peak_load["max"] * 1e6
        df_peak_load["peak_load_in_w"] = (
            df_peak_load["max"] * df_peak_load["profile_share"]
        )
        log.info(f"Peak load for {scenario} determined!")

        # TODO remove after #772
        df_peak_load.rename(columns={"id": "building_id"}, inplace=True)
        df_peak_load["sector"] = "cts"

        # # Write peak loads into db
        write_table_to_postgres(
            df_peak_load,
            BuildingHeatPeakLoads,
            drop=False,
            index=False,
            if_exists="append",
        )

        log.info(f"Peak load for {scenario} exported to DB!")


def assign_voltage_level_to_buildings():
    """
    Add voltage level to all buildings by summed peak demand.

    All entries with same building id get the voltage level corresponding
    to their summed residential and cts peak demand.
    """

    with db.session_scope() as session:
        cells_query = session.query(BuildingElectricityPeakLoads)

        df_peak_loads = pd.read_sql(
            cells_query.statement,
            cells_query.session.bind,
        )

    df_peak_load_buildings = df_peak_loads.groupby(
        ["building_id", "scenario"]
    )["peak_load_in_w"].sum()
    df_peak_load_buildings = df_peak_load_buildings.to_frame()
    df_peak_load_buildings.loc[:, "voltage_level"] = 0

    # Identify voltage_level by thresholds defined in the eGon project
    df_peak_load_buildings.loc[
        df_peak_load_buildings["peak_load_in_w"] <= 0.1 * 1e6, "voltage_level"
    ] = 7
    df_peak_load_buildings.loc[
        df_peak_load_buildings["peak_load_in_w"] > 0.1 * 1e6, "voltage_level"
    ] = 6
    df_peak_load_buildings.loc[
        df_peak_load_buildings["peak_load_in_w"] > 0.2 * 1e6, "voltage_level"
    ] = 5
    df_peak_load_buildings.loc[
        df_peak_load_buildings["peak_load_in_w"] > 5.5 * 1e6, "voltage_level"
    ] = 4
    df_peak_load_buildings.loc[
        df_peak_load_buildings["peak_load_in_w"] > 20 * 1e6, "voltage_level"
    ] = 3
    df_peak_load_buildings.loc[
        df_peak_load_buildings["peak_load_in_w"] > 120 * 1e6, "voltage_level"
    ] = 1

    df_peak_load = pd.merge(
        left=df_peak_loads.drop(columns="voltage_level"),
        right=df_peak_load_buildings["voltage_level"],
        how="left",
        left_on=["building_id", "scenario"],
        right_index=True,
    )

    # Write peak loads into db
    # remove table and replace by new
    write_table_to_postgres(
        df_peak_load,
        BuildingElectricityPeakLoads,
        drop=True,
        index=False,
        if_exists="append",
    )

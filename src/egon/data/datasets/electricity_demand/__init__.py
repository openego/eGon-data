"""The central module containing all code dealing with processing
 data from demandRegio

"""
from sqlalchemy import Column, Float, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.electricity_demand.temporal import insert_cts_load
from egon.data.datasets.electricity_demand_timeseries.hh_buildings import (
    HouseholdElectricityProfilesOfBuildings,
    get_iee_hh_demand_profiles_raw,
)
from egon.data.datasets.electricity_demand_timeseries.hh_profiles import (
    HouseholdElectricityProfilesInCensusCells,
)
from egon.data.datasets.zensus_vg250 import DestatisZensusPopulationPerHa
import egon.data.config

# will be later imported from another file ###
Base = declarative_base()
engine = db.engine()


class HouseholdElectricityDemand(Dataset):
    """
    Create table and store data on household electricity demands per census cell

    Create a table to store the annual electricity demand for households on census cell level.
    In a next step the annual electricity demand per cell is determined by
    executing function :py:func:`get_annual_household_el_demand_cells`

    *Dependencies*
      * :py:class:`DemandRegio <egon.data.datasets.demandregio>`
      * :py:class:`DataBundle <egon.data.datasets.data_bundle.DataBundle>`

    *Resulting tables*
      * :py:class:`demand.egon_demandregio_zensus_electricity <egon.data.datasets.electricity_demand.EgonDemandRegioZensusElectricity>` is created and filled

    """

    #:
    name: str = "HouseholdElectricityDemand"
    #:
    version: str = "0.0.5"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(create_tables, get_annual_household_el_demand_cells),
        )


class CtsElectricityDemand(Dataset):
    """
    Create table and store data on cts electricity demands per census cell

    Creates a table to store data on electricity demands from the cts sector on
    census cell level. For a spatial distribution of electricity demands data
    from DemandRegio, which provides the data on NUT3-level, is used and
    distributed to census cells according to heat demand data from Peta.
    Annual demands are then aggregated per MV grid district and a corresponding
    time series is created taking the shares of different cts branches and their
    specific standard load profiles into account.


    *Dependencies*
      * :py:class:`MapZensusGridDistricts <egon.data.datasets.zensus_mv_grid_districts.MapZensusGridDistricts>`
      * :py:class:`DemandRegio <egon.data.datasets.demandregio.DemandRegio>`
      * :py:class:`HeatDemandImport <egon.data.datasets.heat_demand.HeatDemandImport>`
      * :py:class:`HouseholdElectricityDemand <egon.data.datasets.electricity_demand.HouseholdElectricityDemand>`
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`ZensusMvGridDistricts <egon.data.datasets.zensus_mv_grid_districts.ZensusMvGridDistricts>`
      * :py:class:`ZensusVg250 <egon.data.datasets.zensus_vg250.ZensusVg250>`


    *Resulting tables*
      * :py:class:`demand.egon_etrago_electricity_cts <egon.data.datasets.electricity_demand.temporal.EgonEtragoElectricityCts>` is created and filled


    """

    #:
    name: str = "CtsElectricityDemand"
    #:
    version: str = "0.0.2"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(distribute_cts_demands, insert_cts_load),
        )


class EgonDemandRegioZensusElectricity(Base):
    __tablename__ = "egon_demandregio_zensus_electricity"
    __table_args__ = {"schema": "demand", "extend_existing": True}
    zensus_population_id = Column(
        Integer, ForeignKey(DestatisZensusPopulationPerHa.id), primary_key=True
    )
    scenario = Column(String(50), primary_key=True)
    sector = Column(String, primary_key=True)
    demand = Column(Float)


def create_tables():
    """Create tables for demandregio data
    Returns
    -------
    None.
    """
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS demand;")
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS society;")
    engine = db.engine()
    EgonDemandRegioZensusElectricity.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonDemandRegioZensusElectricity.__table__.create(
        bind=engine, checkfirst=True
    )


def get_annual_household_el_demand_cells():
    """
    Annual electricity demand per cell is determined

    Timeseries for every cell are accumulated, the maximum value
    determined and with the respective nuts3 factor scaled for 2035 and 2050
    scenario.

    Note
    ----------
    In test-mode 'SH' the iteration takes place by 'cell_id' to avoid
    intensive RAM usage. For whole Germany 'nuts3' are taken and
    RAM > 32GB is necessary.
    """

    with db.session_scope() as session:
        cells_query = (
            session.query(
                HouseholdElectricityProfilesOfBuildings,
                HouseholdElectricityProfilesInCensusCells.nuts3,
                HouseholdElectricityProfilesInCensusCells.factor_2019,
                HouseholdElectricityProfilesInCensusCells.factor_2023,
                HouseholdElectricityProfilesInCensusCells.factor_2035,
                HouseholdElectricityProfilesInCensusCells.factor_2050,
            )
            .filter(
                HouseholdElectricityProfilesOfBuildings.cell_id
                == HouseholdElectricityProfilesInCensusCells.cell_id
            )
            .order_by(HouseholdElectricityProfilesOfBuildings.id)
        )

    df_buildings_and_profiles = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col="id"
    )

    # Read demand profiles from egon-data-bundle
    df_profiles = get_iee_hh_demand_profiles_raw()

    def ve(s):
        raise (ValueError(s))

    dataset = egon.data.config.settings()["egon-data"]["--dataset-boundary"]
    scenarios = egon.data.config.settings()["egon-data"]["--scenarios"]

    iterate_over = (
        "nuts3"
        if dataset == "Everything"
        else "cell_id"
        if dataset == "Schleswig-Holstein"
        else ve(f"'{dataset}' is not a valid dataset boundary.")
    )

    df_annual_demand = pd.DataFrame(
        columns=scenarios + ["zensus_population_id"]
    )

    for _, df in df_buildings_and_profiles.groupby(by=iterate_over):
        df_annual_demand_iter = pd.DataFrame(
            columns=scenarios + ["zensus_population_id"]
        )

        if "eGon2035" in scenarios:
            df_annual_demand_iter["eGon2035"] = (
                df_profiles.loc[:, df["profile_id"]].sum(axis=0)
                * df["factor_2035"].values
            )
        if "eGon100RE" in scenarios:
            df_annual_demand_iter["eGon100RE"] = (
                df_profiles.loc[:, df["profile_id"]].sum(axis=0)
                * df["factor_2050"].values
            )
        if "status2019" in scenarios:
            df_annual_demand_iter["status2019"] = (
                df_profiles.loc[:, df["profile_id"]].sum(axis=0)
                * df["factor_2019"].values
            )

        if "status2023" in scenarios:
            df_annual_demand_iter["status2023"] = (
                df_profiles.loc[:, df["profile_id"]].sum(axis=0)
                * df["factor_2023"].values
            )
        df_annual_demand_iter["zensus_population_id"] = df["cell_id"].values
        df_annual_demand = pd.concat([df_annual_demand, df_annual_demand_iter])

    df_annual_demand = (
        df_annual_demand.groupby("zensus_population_id").sum().reset_index()
    )
    df_annual_demand["sector"] = "residential"
    df_annual_demand = df_annual_demand.melt(
        id_vars=["zensus_population_id", "sector"],
        var_name="scenario",
        value_name="demand",
    )
    # convert from Wh to MWh
    df_annual_demand["demand"] = df_annual_demand["demand"] / 1e6

    # delete all cells for residentials
    with db.session_scope() as session:
        session.query(EgonDemandRegioZensusElectricity).filter(
            EgonDemandRegioZensusElectricity.sector == "residential"
        ).delete()

    # Insert data to target table
    df_annual_demand.to_sql(
        name=EgonDemandRegioZensusElectricity.__table__.name,
        schema=EgonDemandRegioZensusElectricity.__table__.schema,
        con=db.engine(),
        index=False,
        if_exists="append",
    )


def distribute_cts_demands():
    """Distribute electrical demands for cts to zensus cells.

    The demands on nuts3-level from demandregio are linear distributed
    to the heat demand of cts in each zensus cell.

    Returns
    -------
    None.

    """

    sources = egon.data.config.datasets()["electrical_demands_cts"]["sources"]

    target = egon.data.config.datasets()["electrical_demands_cts"]["targets"][
        "cts_demands_zensus"
    ]

    db.execute_sql(
        f"""DELETE FROM {target['schema']}.{target['table']}
                   WHERE sector = 'service'"""
    )

    # Select match between zensus cells and nuts3 regions of vg250
    map_nuts3 = db.select_dataframe(
        f"""SELECT zensus_population_id, vg250_nuts3 as nuts3 FROM
        {sources['map_zensus_vg250']['schema']}.
        {sources['map_zensus_vg250']['table']}""",
        index_col="zensus_population_id",
    )

    # Insert data per scenario
    for scn in egon.data.config.settings()["egon-data"]["--scenarios"]:
        # Select heat_demand per zensus cell
        peta = db.select_dataframe(
            f"""SELECT zensus_population_id, demand as heat_demand,
            sector, scenario FROM
            {sources['heat_demand_cts']['schema']}.
            {sources['heat_demand_cts']['table']}
            WHERE scenario = '{scn}'
            AND sector = 'service'""",
            index_col="zensus_population_id",
        )

        # Add nuts3 key to zensus cells
        peta["nuts3"] = map_nuts3.nuts3

        # Calculate share of nuts3 heat demand per zensus cell
        for nuts3, df in peta.groupby("nuts3"):
            peta.loc[df.index, "share"] = (
                df["heat_demand"] / df["heat_demand"].sum()
            )

        # Select forecasted electrical demands from demandregio table
        demand_nuts3 = db.select_dataframe(
            f"""SELECT nuts3, SUM(demand) as demand FROM
            {sources['demandregio']['schema']}.
            {sources['demandregio']['table']}
            WHERE scenario = '{scn}'
            AND wz IN (
                SELECT wz FROM
                {sources['demandregio_wz']['schema']}.
                {sources['demandregio_wz']['table']}
                WHERE sector = 'CTS')
            GROUP BY nuts3""",
            index_col="nuts3",
        )

        # Scale demands on nuts3 level linear to heat demand share
        peta["demand"] = peta["share"].mul(
            demand_nuts3.demand[peta["nuts3"]].values
        )

        # Rename index
        peta.index = peta.index.rename("zensus_population_id")

        # Insert data to target table
        peta[["scenario", "demand", "sector"]].to_sql(
            target["table"],
            schema=target["schema"],
            con=db.engine(),
            if_exists="append",
        )

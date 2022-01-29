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
    def __init__(self, dependencies):
        super().__init__(
            name="HouseholdElectricityDemand",
            version="0.0.2",
            dependencies=dependencies,
            tasks=(create_tables, distribute_household_demands),
        )


class CtsElectricityDemand(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="CtsElectricityDemand",
            version="0.0.2",
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


def distribute_household_demands():
    """Distribute electrical demands for households to zensus cells.

    The demands on nuts3-level from demandregio are linear distributed
    to the share of future population in each zensus cell.

    Returns
    -------
    None.

    """

    sources = egon.data.config.datasets()["electrical_demands_households"][
        "sources"
    ]

    target = egon.data.config.datasets()["electrical_demands_households"][
        "targets"
    ]["household_demands_zensus"]

    db.execute_sql(
        f"""DELETE FROM {target['schema']}.{target['table']}
                   WHERE sector = 'residential'"""
    )

    # Select match between zensus cells and nuts3 regions of vg250
    map_nuts3 = db.select_dataframe(
        f"""SELECT zensus_population_id, vg250_nuts3 as nuts3 FROM
        {sources['map_zensus_vg250']['schema']}.
        {sources['map_zensus_vg250']['table']}""",
        index_col="zensus_population_id",
    )

    # Insert data per scenario
    for scn in sources["demandregio"]["scenarios"]:

        # Set target years per scenario
        if scn == "eGon2035":
            year = 2035
        elif scn == "eGon100RE":
            year = 2050
        else:
            print(f"Warning: Scenario {scn} can not be imported.")

        # Select prognosed population per zensus cell
        zensus = db.select_dataframe(
            f"""SELECT * FROM
            {sources['population_prognosis_zensus']['schema']}.
            {sources['population_prognosis_zensus']['table']}
            WHERE year = {year}
            AND population > 0""",
            index_col="zensus_population_id",
        )

        # Add nuts3 key to zensus cells
        zensus["nuts3"] = map_nuts3.nuts3

        # Calculate share of nuts3 population per zensus cell
        zensus["population_share"] = zensus.population.groupby(
            zensus.nuts3
        ).apply(lambda grp: grp / grp.sum())

        # Select forecasted electrical demands from demandregio table
        demand_nuts3 = db.select_dataframe(
            f"""SELECT nuts3, SUM(demand) as demand FROM
            {sources['demandregio']['schema']}.
            {sources['demandregio']['table']}
            WHERE scenario = '{scn}'
            GROUP BY nuts3""",
            index_col="nuts3",
        )

        # Scale demands on nuts3 level linear to population share
        zensus["demand"] = zensus["population_share"].mul(
            demand_nuts3.demand[zensus["nuts3"]].values
        )

        # Set scenario name and sector
        zensus["scenario"] = scn
        zensus["sector"] = "residential"

        # Rename index
        zensus.index = zensus.index.rename("zensus_population_id")

        # Insert data to target table
        zensus[["scenario", "demand", "sector"]].to_sql(
            target["table"],
            schema=target["schema"],
            con=db.engine(),
            if_exists="append",
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

        dataset = egon.data.config.settings()["egon-data"][
            "--dataset-boundary"
        ]
        iterate_over = (
            "nuts3"
            if dataset == "Everything"
            else "cell_id"
            if dataset == "Schleswig-Holstein"
            else ve(f"'{dataset}' is not a valid dataset boundary.")
        )

        df_annual_demand = pd.DataFrame()

        for nuts3, df in df_buildings_and_profiles.groupby(by=iterate_over):
            df_annual_demand_nuts3 = df_profiles.loc[:, df.profile_id]

            m_index = pd.MultiIndex.from_arrays(
                [df.profile_id, df.building_id],
                names=("profile_id", "cell_id"),
            )
            df_annual_demand_nuts3.columns = m_index
            df_annual_demand_nuts3 = df_annual_demand_nuts3.sum(
                level="cell_id", axis=1
            ).sum()

            df_annual_demand_nuts3 = pd.DataFrame(
                [
                    df_annual_demand_nuts3 * df["factor_2035"].unique(),
                    df_annual_demand_nuts3 * df["factor_2050"].unique(),
                ],
                index=[
                    "building_peak_load_in_w_2035",
                    "building_peak_load_in_w_2050",
                ],
            ).T

            df_annual_demand = pd.concat(
                [df_annual_demand, df_annual_demand_nuts3], axis=0
            )

        df_annual_demand.reset_index(inplace=True)

        EgonDemandRegioZensusElectricity.__table__.drop(
            bind=engine, checkfirst=True
        )
        EgonDemandRegioZensusElectricity.__table__.create(
            bind=engine, checkfirst=True
        )

        # Write peak loads into db
        with db.session_scope() as session:
            session.bulk_insert_mappings(
                EgonDemandRegioZensusElectricity,
                df_annual_demand.to_dict(orient="records"),
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
    for scn in sources["demandregio"]["scenarios"]:

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
        peta["share"] = peta.heat_demand.groupby(peta.nuts3).apply(
            lambda grp: grp / grp.sum()
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

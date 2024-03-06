"""The central module containing all code dealing with importing and
adjusting data from demandRegio

"""
from pathlib import Path
import os
import zipfile

from sqlalchemy import ARRAY, Column, Float, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import numpy as np
import pandas as pd

from egon.data import db, logger
from egon.data.datasets import Dataset
from egon.data.datasets.demandregio.install_disaggregator import (
    clone_and_install,
)
from egon.data.datasets.scenario_parameters import (
    EgonScenario,
    get_sector_parameters,
)
from egon.data.datasets.zensus import download_and_check
import egon.data.config
import egon.data.datasets.scenario_parameters.parameters as scenario_parameters

try:
    from disaggregator import config, data, spatial, temporal

except ImportError as e:
    pass

# will be later imported from another file ###
Base = declarative_base()


class DemandRegio(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="DemandRegio",
            version="0.0.7",
            dependencies=dependencies,
            tasks=(
                #clone_and_install, demandregio must be previously installed
                get_cached_tables,  # adhoc workaround #180
                create_tables,
                {
                    insert_household_demand,
                    insert_society_data,
                    insert_cts_ind_demands,
                },
            ),
        )

class DemandRegioLoadProfiles(Base):
    __tablename__ = "demandregio_household_load_profiles"
    __table_args__ = {"schema": "demand"}

    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    nuts3 = Column(String)
    load_in_mwh = Column(ARRAY(Float()))

class EgonDemandRegioHH(Base):
    __tablename__ = "egon_demandregio_hh"
    __table_args__ = {"schema": "demand"}
    nuts3 = Column(String(5), primary_key=True)
    hh_size = Column(Integer, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    year = Column(Integer)
    demand = Column(Float)


class EgonDemandRegioCtsInd(Base):
    __tablename__ = "egon_demandregio_cts_ind"
    __table_args__ = {"schema": "demand"}
    nuts3 = Column(String(5), primary_key=True)
    wz = Column(Integer, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    year = Column(Integer)
    demand = Column(Float)


class EgonDemandRegioPopulation(Base):
    __tablename__ = "egon_demandregio_population"
    __table_args__ = {"schema": "society"}
    nuts3 = Column(String(5), primary_key=True)
    year = Column(Integer, primary_key=True)
    population = Column(Float)


class EgonDemandRegioHouseholds(Base):
    __tablename__ = "egon_demandregio_household"
    __table_args__ = {"schema": "society"}
    nuts3 = Column(String(5), primary_key=True)
    hh_size = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    households = Column(Integer)


class EgonDemandRegioWz(Base):
    __tablename__ = "egon_demandregio_wz"
    __table_args__ = {"schema": "demand"}
    wz = Column(Integer, primary_key=True)
    sector = Column(String(50))
    definition = Column(String(150))


class EgonDemandRegioTimeseriesCtsInd(Base):
    __tablename__ = "egon_demandregio_timeseries_cts_ind"
    __table_args__ = {"schema": "demand"}
    wz = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    slp = Column(String(50))
    load_curve = Column(ARRAY(Float()))


def create_tables():
    """Create tables for demandregio data
    Returns
    -------
    None.
    """
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS demand;")
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS society;")
    engine = db.engine()
    EgonDemandRegioHH.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioCtsInd.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioPopulation.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioHouseholds.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioWz.__table__.create(bind=engine, checkfirst=True)
    DemandRegioLoadProfiles.__table__.create(bind=db.engine(), checkfirst=True)
    EgonDemandRegioTimeseriesCtsInd.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonDemandRegioTimeseriesCtsInd.__table__.create(
        bind=engine, checkfirst=True
    )


def data_in_boundaries(df):
    """Select rows with nuts3 code within boundaries, used for testmode

    Parameters
    ----------
    df : pandas.DataFrame
        Data for all nuts3 regions

    Returns
    -------
    pandas.DataFrame
        Data for nuts3 regions within boundaries

    """
    engine = db.engine()

    df = df.reset_index()

    # Change nuts3 region names to 2016 version
    nuts_names = {"DEB16": "DEB1C", "DEB19": "DEB1D"}
    df.loc[df.nuts3.isin(nuts_names), "nuts3"] = df.loc[
        df.nuts3.isin(nuts_names), "nuts3"
    ].map(nuts_names)

    df = df.set_index("nuts3")

    return df[
        df.index.isin(
            pd.read_sql(
                "SELECT DISTINCT ON (nuts) nuts FROM boundaries.vg250_krs",
                engine,
            ).nuts
        )
    ]


def insert_cts_ind_wz_definitions():
    """Insert demandregio's definitions of CTS and industrial branches

    Returns
    -------
    None.

    """

    source = egon.data.config.datasets()["demandregio_cts_ind_demand"][
        "sources"
    ]

    target = egon.data.config.datasets()["demandregio_cts_ind_demand"][
        "targets"
    ]["wz_definitions"]

    engine = db.engine()

    for sector in source["wz_definitions"]:
        file_path = (
            Path(".")
            / "data_bundle_egon_data"
            / "WZ_definition"
            / source["wz_definitions"][sector]
        )

        if sector == "CTS":
            delimiter = ";"
        else:
            delimiter = ","
        df = (
            pd.read_csv(file_path, delimiter=delimiter, header=None)
            .rename({0: "wz", 1: "definition"}, axis="columns")
            .set_index("wz")
        )
        df["sector"] = sector
        df.to_sql(
            target["table"],
            engine,
            schema=target["schema"],
            if_exists="append",
        )


def match_nuts3_bl():
    """Function that maps the federal state to each nuts3 region

    Returns
    -------
    df : pandas.DataFrame
        List of nuts3 regions and the federal state of Germany.

    """

    engine = db.engine()

    df = pd.read_sql(
        "SELECT DISTINCT ON (boundaries.vg250_krs.nuts) "
        "boundaries.vg250_krs.nuts, boundaries.vg250_lan.gen "
        "FROM boundaries.vg250_lan, boundaries.vg250_krs "
        " WHERE ST_CONTAINS("
        "boundaries.vg250_lan.geometry, "
        "boundaries.vg250_krs.geometry)",
        con=engine,
    )

    df.gen[df.gen == "Baden-Württemberg (Bodensee)"] = "Baden-Württemberg"
    df.gen[df.gen == "Bayern (Bodensee)"] = "Bayern"

    return df.set_index("nuts")


def adjust_ind_pes(ec_cts_ind):
    """
    Adjust electricity demand of industrial consumers due to electrification
    of process heat based on assumptions of pypsa-eur-sec.

    Parameters
    ----------
    ec_cts_ind : pandas.DataFrame
        Industrial demand without additional electrification

    Returns
    -------
    ec_cts_ind : pandas.DataFrame
        Industrial demand with additional electrification

    """

    pes_path = (
        Path(".") / "data_bundle_egon_data" / "pypsa_eur_sec" / "resources"
    )

    sources = egon.data.config.datasets()["demandregio_cts_ind_demand"][
        "sources"
    ]["new_consumers_2050"]

    # Extract today's industrial demand from pypsa-eur-sec
    demand_today = pd.read_csv(
        pes_path / sources["pes-demand-today"],
        header=None,
    ).transpose()

    # Filter data
    demand_today[1].fillna("carrier", inplace=True)
    demand_today = demand_today[
        (demand_today[0] == "DE") | (demand_today[1] == "carrier")
    ].drop([0, 2], axis="columns")

    demand_today = (
        demand_today.transpose()
        .set_index(0)
        .transpose()
        .set_index("carrier")
        .transpose()
        .loc["electricity"]
        .astype(float)
    )

    # Calculate future industrial demand from pypsa-eur-sec
    # based on production and energy demands per carrier ('sector ratios')
    prod_tomorrow = pd.read_csv(pes_path / sources["pes-production-tomorrow"])

    prod_tomorrow = prod_tomorrow[prod_tomorrow["kton/a"] == "DE"].set_index(
        "kton/a"
    )

    sector_ratio = (
        pd.read_csv(pes_path / sources["pes-sector-ratios"])
        .set_index("MWh/tMaterial")
        .loc["elec"]
    )

    demand_tomorrow = prod_tomorrow.multiply(
        sector_ratio.div(1000)
    ).transpose()["DE"]

    # Calculate changes of electrical demand per sector in pypsa-eur-sec
    change = pd.DataFrame(
        (demand_tomorrow / demand_today)
        / (demand_tomorrow / demand_today).sum()
    )

    # Drop rows without changes
    change = change[~change[0].isnull()]

    # Map industrial branches of pypsa-eur-sec to WZ2008 used in demandregio
    change["wz"] = change.index.map(
        {
            "Alumina production": 24,
            "Aluminium - primary production": 24,
            "Aluminium - secondary production": 24,
            "Ammonia": 20,
            "Basic chemicals (without ammonia)": 20,
            "Cement": 23,
            "Ceramics & other NMM": 23,
            "Electric arc": 24,
            "Food, beverages and tobacco": 10,
            "Glass production": 23,
            "Integrated steelworks": 24,
            "Machinery Equipment": 28,
            "Other Industrial Sectors": 32,
            "Other chemicals": 20,
            "Other non-ferrous metals": 24,
            "Paper production": 17,
            "Pharmaceutical products etc.": 21,
            "Printing and media reproduction": 18,
            "Pulp production": 17,
            "Textiles and leather": 13,
            "Transport Equipment": 29,
            "Wood and wood products": 16,
        }
    )

    # Group by WZ2008
    shares_per_wz = change.groupby("wz")[0].sum()

    # Calculate addtional demands needed to meet future demand of pypsa-eur-sec
    addtional_mwh = shares_per_wz.multiply(
        demand_tomorrow.sum() * 1000000 - ec_cts_ind.sum().sum()
    )

    # Calulate overall industrial demand for eGon100RE
    final_mwh = addtional_mwh + ec_cts_ind[addtional_mwh.index].sum()

    # Linear scale the industrial demands per nuts3 and wz to meet final demand
    ec_cts_ind[addtional_mwh.index] *= (
        final_mwh / ec_cts_ind[addtional_mwh.index].sum()
    )

    return ec_cts_ind


def adjust_cts_ind_nep(ec_cts_ind, sector):
    """Add electrical demand of new largescale CTS und industrial consumers
    according to NEP 2021, scneario C 2035. Values per federal state are
    linear distributed over all CTS branches and nuts3 regions.

    Parameters
    ----------
    ec_cts_ind : pandas.DataFrame
        CTS or industry demand without new largescale consumers.

    Returns
    -------
    ec_cts_ind : pandas.DataFrame
        CTS or industry demand including new largescale consumers.

    """
    sources = egon.data.config.datasets()["demandregio_cts_ind_demand"][
        "sources"
    ]

    file_path = (
        Path(".")
        / "data_bundle_egon_data"
        / "nep2035_version2021"
        / sources["new_consumers_2035"]
    )

    # get data from NEP per federal state
    new_con = pd.read_csv(file_path, delimiter=";", decimal=",", index_col=0)

    # match nuts3 regions to federal states
    groups = ec_cts_ind.groupby(match_nuts3_bl().gen)

    # update demands per federal state
    for group in groups.indices.keys():
        g = groups.get_group(group)
        data_new = g.mul(1 + new_con[sector][group] * 1e6 / g.sum().sum())
        ec_cts_ind[ec_cts_ind.index.isin(g.index)] = data_new

    return ec_cts_ind


def disagg_households_power(
    scenario, year, weight_by_income=False, original=False, **kwargs
):
    """
    Perform spatial disaggregation of electric power in [GWh/a] by key and
    possibly weight by income.
    Similar to disaggregator.spatial.disagg_households_power


    Parameters
    ----------
    by : str
        must be one of ['households', 'population']
    weight_by_income : bool, optional
        Flag if to weight the results by the regional income (default False)
    orignal : bool, optional
        Throughput to function households_per_size,
        A flag if the results should be left untouched and returned in
        original form for the year 2011 (True) or if they should be scaled to
        the given `year` by the population in that year (False).

    Returns
    -------
    pd.DataFrame or pd.Series
    """
    # source: survey of energieAgenturNRW
    demand_per_hh_size = pd.DataFrame(
        index=range(1, 7),
        data={
            "weighted DWH": [2290, 3202, 4193, 4955, 5928, 5928],
            "without DHW": [1714, 2812, 3704, 4432, 5317, 5317],
        },
    )

    # Bottom-Up: Power demand by household sizes in [MWh/a] for each scenario
    if scenario in ["status2019", "eGon2021", "eGon2035"]:
        # chose demand per household size from survey including weighted DHW
        power_per_HH = demand_per_hh_size["weighted DWH"] / 1e3

        # calculate demand per nuts3
        df = (
            data.households_per_size(original=original, year=year)
            * power_per_HH
        )

        if scenario == "eGon2035":
            # scale to fit demand of NEP 2021 scebario C 2035 (119TWh)
            df *= 119000000 / df.sum().sum()

    elif scenario == "eGon100RE":
        # chose demand per household size from survey without DHW
        power_per_HH = demand_per_hh_size["without DHW"] / 1e3

        # calculate demand per nuts3 in 2011
        df_2011 = data.households_per_size(year=2011) * power_per_HH

        # scale demand per hh-size to meet demand without heat
        # according to JRC in 2011 (136.6-(20.14+9.41) TWh)
        power_per_HH *= (136.6 - (20.14 + 9.41)) * 1e6 / df_2011.sum().sum()

        # calculate demand per nuts3 in 2050
        df = data.households_per_size(year=year) * power_per_HH

    else:
        print(
            f"Electric demand per household size for scenario {scenario} "
            "is not specified."
        )

    if weight_by_income:
        df = spatial.adjust_by_income(df=df)

    return df

def write_demandregio_hh_profiles_to_db(hh_profiles, year):
    """Write HH demand profiles from demand regio into db. One row per
    year and nuts3. The annual load profile timeseries is an array.

    schema: demand
    tablename: demandregio_household_load_profiles



    Parameters
    ----------
    hh_profiles: pd.DataFrame
    year: int

    Returns
    -------
    """
    df_to_db = pd.DataFrame(columns=["id", "year", "nuts3", "load_in_mwh"]).set_index("id")
    dataset = egon.data.config.settings()["egon-data"]["--dataset-boundary"]

    if dataset == "Schleswig-Holstein":
        hh_profiles = hh_profiles.loc[
            :, hh_profiles.columns.str.contains("DEF0")]

    id = 0
    for nuts3 in hh_profiles.columns:
        id+=1
        df_to_db.at[id, "year"] = year
        df_to_db.at[id, "nuts3"] = nuts3
        df_to_db.at[id, "load_in_mwh"] = hh_profiles[nuts3].to_list()

    df_to_db["year"] = df_to_db["year"].apply(int)
    df_to_db["nuts3"] = df_to_db["nuts3"].astype(str)
    df_to_db["load_in_mwh"] = df_to_db["load_in_mwh"].apply(list)
    df_to_db = df_to_db.reset_index()

    df_to_db.to_sql(
        name=DemandRegioLoadProfiles.__table__.name,
        schema=DemandRegioLoadProfiles.__table__.schema,
        con=db.engine(),
        if_exists="append",
        index=-False,
    )

    return

def insert_hh_demand(scenario, year, engine):
    """Calculates electrical demands of private households using demandregio's
    disaggregator and insert results into the database.

    Parameters
    ----------
    scenario : str
        Name of the corresponing scenario.
    year : int
        The number of households per region is taken from this year.

    Returns
    -------
    None.

    """
    targets = egon.data.config.datasets()["demandregio_household_demand"][
        "targets"
    ]["household_demand"]
    # get demands of private households per nuts and size from demandregio
    ec_hh = disagg_households_power(scenario, year)

    # Select demands for nuts3-regions in boundaries (needed for testmode)
    ec_hh = data_in_boundaries(ec_hh)

    # insert into database
    for hh_size in ec_hh.columns:
        df = pd.DataFrame(ec_hh[hh_size])
        df["year"] = year
        df["scenario"] = scenario
        df["hh_size"] = hh_size
        df = df.rename({hh_size: "demand"}, axis="columns")
        df.to_sql(
            targets["table"],
            engine,
            schema=targets["schema"],
            if_exists="append",
        )

    # insert housholds demand timeseries
    try:
        hh_load_timeseries = (
            temporal.disagg_temporal_power_housholds_slp(
                use_nuts3code=True,
                by="households",
                weight_by_income=False,
                year=year,
            )
            .resample("h")
            .sum()
        )
        hh_load_timeseries.rename(
            columns={"DEB16": "DEB1C", "DEB19": "DEB1D"}, inplace = True)
    except:
        logger.info("HH demand timeseries could not be imported. Using BK")
        hh_load_timeseries = pd.read_pickle(
            "df_load_profiles.pkl"
        )

    write_demandregio_hh_profiles_to_db(hh_load_timeseries, year)


def insert_cts_ind(scenario, year, engine, target_values):
    """Calculates electrical demands of CTS and industry using demandregio's
    disaggregator, adjusts them according to resulting values of NEP 2021 or
    JRC IDEES and insert results into the database.

    Parameters
    ----------
    scenario : str
        Name of the corresponing scenario.
    year : int
        The number of households per region is taken from this year.
    target_values : dict
        List of target values for each scenario and sector.

    Returns
    -------
    None.

    """

    targets = egon.data.config.datasets()["demandregio_cts_ind_demand"][
        "targets"
    ]

    for sector in ["CTS", "industry"]:
        # get demands per nuts3 and wz of demandregio
        ec_cts_ind = spatial.disagg_CTS_industry(
            use_nuts3code=True, source="power", sector=sector, year=year
        ).transpose()

        ec_cts_ind.index = ec_cts_ind.index.rename("nuts3")

        # exclude mobility sector from GHD
        ec_cts_ind = ec_cts_ind.drop(columns=49, errors="ignore")

        # scale values according to target_values
        if sector in target_values[scenario].keys():
            ec_cts_ind *= (
                target_values[scenario][sector] * 1e3 / ec_cts_ind.sum().sum()
            )

        # include new largescale consumers according to NEP 2021
        if scenario == "eGon2035":
            ec_cts_ind = adjust_cts_ind_nep(ec_cts_ind, sector)
        # include new industrial demands due to sector coupling
        if (scenario == "eGon100RE") & (sector == "industry"):
            ec_cts_ind = adjust_ind_pes(ec_cts_ind)

        # Select demands for nuts3-regions in boundaries (needed for testmode)
        ec_cts_ind = data_in_boundaries(ec_cts_ind)

        # insert into database
        for wz in ec_cts_ind.columns:
            df = pd.DataFrame(ec_cts_ind[wz])
            df["year"] = year
            df["wz"] = wz
            df["scenario"] = scenario
            df = df.rename({wz: "demand"}, axis="columns")
            df.index = df.index.rename("nuts3")
            df.to_sql(
                targets["cts_ind_demand"]["table"],
                engine,
                targets["cts_ind_demand"]["schema"],
                if_exists="append",
            )


def insert_household_demand():
    """Insert electrical demands for households according to
    demandregio using its disaggregator-tool in MWh

    Returns
    -------
    None.

    """
    targets = egon.data.config.datasets()["demandregio_household_demand"][
        "targets"
    ]
    engine = db.engine()

    scenarios = egon.data.config.settings()["egon-data"]["--scenarios"]

    scenarios.append("eGon2021")

    for t in targets:
        db.execute_sql(
            f"DELETE FROM {targets[t]['schema']}.{targets[t]['table']};"
        )

    for scn in scenarios:
        year = scenario_parameters.global_settings(scn)["population_year"]

        # Insert demands of private households
        insert_hh_demand(scn, year, engine)


def insert_cts_ind_demands():
    """Insert electricity demands per nuts3-region in Germany according to
    demandregio using its disaggregator-tool in MWh

    Returns
    -------
    None.

    """
    targets = egon.data.config.datasets()["demandregio_cts_ind_demand"][
        "targets"
    ]
    engine = db.engine()

    for t in targets:
        db.execute_sql(
            f"DELETE FROM {targets[t]['schema']}.{targets[t]['table']};"
        )

    insert_cts_ind_wz_definitions()

    scenarios = egon.data.config.settings()["egon-data"]["--scenarios"]

    scenarios.append("eGon2021")

    ###########################################################################
    # Workaround to get the data of the table demand.egon_demandregio_cts_ind
    # since demand-regio is not available, the data retreived from cached files
    # is producing extream peaks that were not there before. This workaround
    # MUST be deleted as soon as the disaggregator is available again.
    # bk_cts_ind = pd.read_pickle("cts_ind_demand")
    # bk_cts_ind.to_sql(
    #     targets["cts_ind_demand"]["table"],
    #     engine,
    #     schema=targets["cts_ind_demand"]["schema"],
    #     index= False,
    #     if_exists= "append")

    for scn in scenarios:
        year = scenario_parameters.global_settings(scn)["population_year"]

        if year > 2035:
            year = 2035

        # target values per scenario in MWh
        target_values = {
            # according to NEP 2021
            # new consumers will be added seperatly
            "eGon2035": {"CTS": 135300, "industry": 225400},
            # CTS: reduce overall demand from demandregio (without traffic)
            # by share of heat according to JRC IDEES, data from 2011
            # industry: no specific heat demand, use data from demandregio
            "eGon100RE": {"CTS": (1 - (5.96 + 6.13) / 154.64) * 125183.403},
            # no adjustments for status quo
            "eGon2021": {},
            "status2019": {},
        }

        insert_cts_ind(scn, year, engine, target_values)
    ###########################################################################

    # Insert load curves per wz
    timeseries_per_wz()


def insert_society_data():
    """Insert population and number of households per nuts3-region in Germany
    according to demandregio using its disaggregator-tool

    Returns
    -------
    None.

    """
    targets = egon.data.config.datasets()["demandregio_society"]["targets"]
    engine = db.engine()

    for t in targets:
        db.execute_sql(
            f"DELETE FROM {targets[t]['schema']}.{targets[t]['table']};"
        )

    target_years = np.append(
        get_sector_parameters("global").population_year.values, 2018
    )

    for year in target_years:
        df_pop = pd.DataFrame(data.population(year=year))
        df_pop["year"] = year
        df_pop = df_pop.rename({"value": "population"}, axis="columns")
        # Select data for nuts3-regions in boundaries (needed for testmode)
        df_pop = data_in_boundaries(df_pop)
        df_pop.to_sql(
            targets["population"]["table"],
            engine,
            schema=targets["population"]["schema"],
            if_exists="append",
        )

    for year in target_years:
        df_hh = pd.DataFrame(data.households_per_size(year=year))
        # Select data for nuts3-regions in boundaries (needed for testmode)
        df_hh = data_in_boundaries(df_hh)
        for hh_size in df_hh.columns:
            df = pd.DataFrame(df_hh[hh_size])
            df["year"] = year
            df["hh_size"] = hh_size
            df = df.rename({hh_size: "households"}, axis="columns")
            df.to_sql(
                targets["household"]["table"],
                engine,
                schema=targets["household"]["schema"],
                if_exists="append",
            )


def insert_timeseries_per_wz(sector, year):
    """Insert normalized electrical load time series for the selected sector

    Parameters
    ----------
    sector : str
        Name of the sector. ['CTS', 'industry']
    year : int
        Selected weather year

    Returns
    -------
    None.

    """
    targets = egon.data.config.datasets()["demandregio_cts_ind_demand"][
        "targets"
    ]

    if sector == "CTS":
        profiles = (
            data.CTS_power_slp_generator("SH", year=year)
            .drop(
                [
                    "Day",
                    "Hour",
                    "DayOfYear",
                    "WD",
                    "SA",
                    "SU",
                    "WIZ",
                    "SOZ",
                    "UEZ",
                ],
                axis="columns",
            )
            .resample("H")
            .sum()
        )
        wz_slp = config.slp_branch_cts_power()
    elif sector == "industry":
        profiles = (
            data.shift_load_profile_generator(state="SH", year=year)
            .resample("H")
            .sum()
        )
        wz_slp = config.shift_profile_industry()

    else:
        print(f"Sector {sector} is not valid.")

    df = pd.DataFrame(
        index=wz_slp.keys(), columns=["slp", "load_curve", "year"]
    )

    df.index.rename("wz", inplace=True)

    df.slp = wz_slp.values()

    df.year = year

    df.load_curve = profiles[df.slp].transpose().values.tolist()

    db.execute_sql(
        f"""
                   DELETE FROM {targets['timeseries_cts_ind']['schema']}.
                   {targets['timeseries_cts_ind']['table']}
                   WHERE wz IN (
                       SELECT wz FROM {targets['wz_definitions']['schema']}.
                       {targets['wz_definitions']['table']}
                       WHERE sector = '{sector}')
                   """
    )

    df.to_sql(
        targets["timeseries_cts_ind"]["table"],
        schema=targets["timeseries_cts_ind"]["schema"],
        con=db.engine(),
        if_exists="append",
    )


def timeseries_per_wz():
    """Calcultae and insert normalized timeseries per wz for cts and industry

    Returns
    -------
    None.

    """

    years = get_sector_parameters("global").weather_year.unique()

    for year in years:
        for sector in ["CTS", "industry"]:
            insert_timeseries_per_wz(sector, int(year))

def get_cached_tables():
    """Get cached demandregio tables and db-dump from former runs"""
    data_config = egon.data.config.datasets()
    for s in ["cache", "dbdump"]:
        url = data_config["demandregio_workaround"]["source"][s]["url"]
        target_path = data_config["demandregio_workaround"]["targets"][s]["path"]
        filename = os.path.basename(url)
        file_path = Path(".", target_path, filename).resolve()
        os.makedirs(file_path.parent, exist_ok=True)
        logger.info(f"Downloading: {filename} from {url}.")
        download_and_check(url, file_path, max_iteration=5)
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(file_path.parent)

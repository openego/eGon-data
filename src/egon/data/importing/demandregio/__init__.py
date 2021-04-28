"""The central module containing all code dealing with importing and
adjusting data from demandRegio

"""
import os
import pandas as pd
import numpy as np
import egon.data.config
import egon.data.importing.scenarios.parameters as scenario_parameters
from egon.data import db
from egon.data.importing.scenarios import get_sector_parameters, EgonScenario
from sqlalchemy import Column, String, Float, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

try:
    from disaggregator import data, spatial
except:
    print(
        "Could not import disaggregator. "
        "Please run task 'demandregio-installation'")
# will be later imported from another file ###
Base = declarative_base()


class EgonDemandRegioHH(Base):
    __tablename__ = 'egon_demandregio_hh'
    __table_args__ = {'schema': 'demand'}
    nuts3 = Column(String(5), primary_key=True)
    hh_size = Column(Integer, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    year = Column(Integer)
    demand = Column(Float)


class EgonDemandRegioCtsInd(Base):
    __tablename__ = 'egon_demandregio_cts_ind'
    __table_args__ = {'schema': 'demand'}
    nuts3 = Column(String(5), primary_key=True)
    wz = Column(Integer, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    year = Column(Integer)
    demand = Column(Float)


class EgonDemandRegioPopulation(Base):
    __tablename__ = 'egon_demandregio_population'
    __table_args__ = {'schema': 'society'}
    nuts3 = Column(String(5), primary_key=True)
    year = Column(Integer, primary_key=True)
    population = Column(Float)


class EgonDemandRegioHouseholds(Base):
    __tablename__ = 'egon_demandregio_household'
    __table_args__ = {'schema': 'society'}
    nuts3 = Column(String(5), primary_key=True)
    hh_size = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    households = Column(Integer)


class EgonDemandRegioWz(Base):
    __tablename__ = 'egon_demandregio_wz'
    __table_args__ = {'schema': 'demand'}
    wz = Column(Integer, primary_key=True)
    sector = Column(String(50))
    definition = Column(String(150))


def create_tables():
    """Create tables for demandregio data
    Returns
    -------
    None.
    """
    db.execute_sql(
        "CREATE SCHEMA IF NOT EXISTS demand;")
    db.execute_sql(
        "CREATE SCHEMA IF NOT EXISTS society;")
    engine = db.engine()
    EgonDemandRegioHH.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioCtsInd.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioPopulation.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioHouseholds.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioWz.__table__.create(bind=engine, checkfirst=True)

def data_in_boundaries(df):
    """ Select rows with nuts3 code within boundaries, used for testmode

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
    nuts_names = {
        'DEB16': 'DEB1C',
        'DEB19': 'DEB1D'}
    df.loc[df.nuts3.isin(nuts_names), 'nuts3'] = df.loc[
        df.nuts3.isin(nuts_names), 'nuts3'].map(nuts_names)

    df = df.set_index('nuts3')

    return df[df.index.isin(pd.read_sql(
        "SELECT DISTINCT ON (nuts) nuts FROM boundaries.vg250_krs",
        engine).nuts)]

def insert_cts_ind_wz_definitions():
    """ Insert demandregio's definitions of CTS and industrial branches

    Returns
    -------
    None.

    """

    source = (egon.data.config.datasets()
               ['demandregio_cts_ind_demand']['sources'])

    target = (egon.data.config.datasets()
               ['demandregio_cts_ind_demand']['targets']['wz_definitions'])

    engine = db.engine()

    for sector in source['wz_definitions']:
        df = pd.read_csv(
            os.path.join(
                os.path.dirname(__file__),
                source['wz_definitions'][sector])).rename(
                    {'WZ': 'wz', 'Name': 'definition'},
                    axis='columns').set_index('wz')
        df['sector'] = sector
        df.to_sql(target['table'],
                  engine,
                  schema=target['schema'],
                  if_exists='append')


def match_nuts3_bl():
    """ Function that maps the federal state to each nuts3 region

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
        con=engine)

    df.gen[df.gen == 'Baden-Württemberg (Bodensee)'] = 'Baden-Württemberg'
    df.gen[df.gen == 'Bayern (Bodensee)'] = 'Bayern'

    return df.set_index('nuts')


def adjust_cts_ind_nep(ec_cts_ind, sector):
    """ Add electrical demand of new largescale CTS und industrial consumers
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
    sources = (egon.data.config.datasets()
               ['demandregio_cts_ind_demand']['sources'])

    # get data from NEP per federal state
    new_con = pd.read_csv(os.path.join(
        os.path.dirname(__file__),
        sources['new_consumers_2035']),
        delimiter=';', decimal=',', index_col=0)

    # match nuts3 regions to federal states
    groups = ec_cts_ind.groupby(match_nuts3_bl().gen)

    # update demands per federal state
    for group in groups.indices.keys():
        g = groups.get_group(group)
        data_new = g.mul(1 + new_con[sector][group] * 1e6 /g.sum().sum())
        ec_cts_ind[ec_cts_ind.index.isin(g.index)] = data_new


    return ec_cts_ind


def disagg_households_power(scenario, year, weight_by_income=False,
                            original=False, **kwargs):
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
    demand_per_hh_size = pd.DataFrame(index=range(1,7), data = {
        'weighted DWH': [2290, 3202, 4193, 4955, 5928, 5928],
        'without DHW': [1714, 2812, 3704, 4432, 5317, 5317]})

    # Bottom-Up: Power demand by household sizes in [MWh/a] for each scenario
    if scenario == 'eGon2035':
        # chose demand per household size from survey including weighted DHW
        power_per_HH = demand_per_hh_size['weighted DWH']/ 1e3

        # calculate demand per nuts3
        df = data.households_per_size(
            original=original, year=year) * power_per_HH

        # scale to fit demand of NEP 2021 scebario C 2035 (119TWh)
        df *= 119000000/df.sum().sum()

    elif scenario == 'eGon100RE':

        # chose demand per household size from survey without DHW
        power_per_HH = demand_per_hh_size['without DHW']/ 1e3

        # calculate demand per nuts3 in 2011
        df_2011 = data.households_per_size(year=2011) * power_per_HH

        # scale demand per hh-size to meet demand without heat
        # according to JRC in 2011 (136.6-(20.14+9.41) TWh)
        power_per_HH *= (136.6-(20.14+9.41))*1e6/df_2011.sum().sum()

        # calculate demand per nuts3 in 2050
        df = data.households_per_size(year=year) * power_per_HH

    else:
        print(f"Electric demand per household size for scenario {scenario} "
              "is not specified.")

    if weight_by_income:
        df = spatial.adjust_by_income(df=df)

    return df


def insert_hh_demand(scenario, year, engine):
    """ Calculates electrical demands of private households using demandregio's
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
    targets = (egon.data.config.datasets()
               ['demandregio_household_demand']['targets']['household_demand'])
    # get demands of private households per nuts and size from demandregio
    ec_hh = disagg_households_power(scenario, year)

    # Select demands for nuts3-regions in boundaries (needed for testmode)
    ec_hh = data_in_boundaries(ec_hh)

    # insert into database
    for hh_size in ec_hh.columns:
        df = pd.DataFrame(ec_hh[hh_size])
        df['year'] = year
        df['scenario'] = scenario
        df['hh_size'] = hh_size
        df = df.rename({hh_size: 'demand'}, axis='columns')
        df.to_sql(targets['table'],
                  engine,
                  schema=targets['schema'],
                  if_exists='append')


def insert_cts_ind(scenario, year, engine, target_values):
    """ Calculates electrical demands of CTS and industry using demandregio's
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

    targets = (egon.data.config.datasets()
               ['demandregio_cts_ind_demand']['targets'])

    for sector in ['CTS', 'industry']:
        # get demands per nuts3 and wz of demandregio
        ec_cts_ind = spatial.disagg_CTS_industry(
                    use_nuts3code=True,
                    source='power',
                    sector=sector,
                    year=year).transpose()

        ec_cts_ind.index = ec_cts_ind.index.rename('nuts3')

        # exclude mobility sector from GHD
        ec_cts_ind = ec_cts_ind.drop(columns=49, errors='ignore')

        # scale values according to target_values
        if sector in target_values[scenario].keys():
            ec_cts_ind *= target_values[scenario][sector]*1e3 / \
                ec_cts_ind.sum().sum()

        # include new largescale consumers according to NEP 2021
        if scenario == 'eGon2035':
            ec_cts_ind = adjust_cts_ind_nep(ec_cts_ind, sector)

        # Select demands for nuts3-regions in boundaries (needed for testmode)
        ec_cts_ind = data_in_boundaries(ec_cts_ind)

        # insert into database
        for wz in ec_cts_ind.columns:
            df = pd.DataFrame(ec_cts_ind[wz])
            df['year'] = year
            df['wz'] = wz
            df['scenario'] = scenario
            df = df.rename({wz: 'demand'}, axis='columns')
            df.index = df.index.rename('nuts3')
            df.to_sql(
               targets['cts_ind_demand']['table'],
                engine,
                targets['cts_ind_demand']['schema'],
                if_exists='append')


def insert_household_demand():
    """ Insert electrical demands for households according to
    demandregio using its disaggregator-tool in MWh

    Returns
    -------
    None.

    """
    targets = (egon.data.config.datasets()
               ['demandregio_household_demand']['targets'])
    engine = db.engine()

    for t in targets:
        db.execute_sql(
                f"DELETE FROM {targets[t]['schema']}.{targets[t]['table']};")

    for scn in ['eGon2035', 'eGon100RE']:

        year = scenario_parameters.global_settings(scn)['population_year']

        # Insert demands of private households
        insert_hh_demand(scn, year, engine)


def insert_cts_ind_demands():
    """ Insert electricity demands per nuts3-region in Germany according to
    demandregio using its disaggregator-tool in MWh

    Returns
    -------
    None.

    """
    targets = (egon.data.config.datasets()
               ['demandregio_cts_ind_demand']['targets'])
    engine = db.engine()

    for t in targets:
        db.execute_sql(
                f"DELETE FROM {targets[t]['schema']}.{targets[t]['table']};")

    insert_cts_ind_wz_definitions()

    for scn in ['eGon2035', 'eGon100RE']:

        year = scenario_parameters.global_settings(scn)['population_year']

        if year > 2035:
            year = 2035

        # target values per scenario in MWh
        target_values = {
            # according to NEP 2021
            # new consumers will be added seperatly
            'eGon2035': {
                'CTS': 135300,
                'industry': 225400},
            # CTS: reduce overall demand from demandregio (without traffic)
            # by share of heat according to JRC IDEES, data from 2011
            # industry: no specific heat demand, use data from demandregio
            'eGon100RE': {
                'CTS': (1-(5.96+6.13)/154.64)*125183.403}}

        insert_cts_ind(scn, year, engine, target_values)


def insert_society_data():
    """ Insert population and number of households per nuts3-region in Germany
    according to demandregio using its disaggregator-tool

    Returns
    -------
    None.

    """
    targets = egon.data.config.datasets()['demandregio_society']['targets']
    engine = db.engine()

    for t in targets:
        db.execute_sql(
                f"DELETE FROM {targets[t]['schema']}.{targets[t]['table']};")

    target_years = np.append(
        get_sector_parameters('global').population_year.values, 2018)

    for year in target_years:
        df_pop = pd.DataFrame(data.population(year=year))
        df_pop['year'] = year
        df_pop = df_pop.rename({'value': 'population'}, axis='columns')
        # Select data for nuts3-regions in boundaries (needed for testmode)
        df_pop = data_in_boundaries(df_pop)
        df_pop.to_sql(targets['population']['table'],
                      engine,
                      schema=targets['population']['schema'],
                      if_exists='append')


    for year in target_years:
        df_hh = pd.DataFrame(data.households_per_size(year=year))
        # Select data for nuts3-regions in boundaries (needed for testmode)
        df_hh = data_in_boundaries(df_hh)
        for hh_size in df_hh.columns:
            df = pd.DataFrame(df_hh[hh_size])
            df['year'] = year
            df['hh_size'] = hh_size
            df = df.rename({hh_size: 'households'}, axis='columns')
            df.to_sql(targets['household']['table'],
                      engine,
                      schema=targets['household']['schema'],
                      if_exists='append')

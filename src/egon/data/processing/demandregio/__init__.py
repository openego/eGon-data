"""The central module containing all code dealing with processing
 data from demandRegio

"""
import egon.data.config
from egon.data import db
from sqlalchemy import Column, String, Float, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from egon.data.processing.zensus_vg250.zensus_population_inside_germany import DestatisZensusPopulationPerHa
# will be later imported from another file ###
Base = declarative_base()

class EgonDemandRegioZensusElectricity(Base):
    __tablename__ = 'egon_demandregio_zensus_electricity'
    __table_args__ = {'schema': 'demand', 'extend_existing':True}
    zensus_population_id =Column(Integer, ForeignKey(
        DestatisZensusPopulationPerHa.id), primary_key=True)
    scenario = Column(String(50), primary_key=True)
    sector = Column(String, primary_key=True)
    demand = Column(Float)


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
    EgonDemandRegioZensusElectricity.__table__.drop(
        bind=engine, checkfirst=True)
    EgonDemandRegioZensusElectricity.__table__.create(
        bind=engine, checkfirst=True)


def distribute_household_demands():
    """ Distribute electrical demands for households to zensus cells.

    The demands on nuts3-level from demandregio are linear distributed
    to the share of future population in each zensus cell.

    Returns
    -------
    None.

    """

    sources = (egon.data.config.datasets()
               ['electrical_demands_households']['sources'])

    target = (egon.data.config.datasets()
              ['electrical_demands_households']['targets']
              ['household_demands_zensus'])

    db.execute_sql(f"DELETE FROM {target['schema']}.{target['table']}")

    # Select match between zensus cells and nuts3 regions of vg250
    map_nuts3 = db.select_dataframe(
        f"""SELECT zensus_population_id, nuts3 FROM
        {sources['map_zensus_vg250']['schema']}.
        {sources['map_zensus_vg250']['table']}""",
        index_col='zensus_population_id')

    # Insert data per scenario
    for scn in sources['demandregio']['scenarios']:

        # Set target years per scenario
        if scn == 'eGon2035':
            year = 2035
        elif scn == 'eGon100RE':
            year = 2050
        else:
            print(f"Warning: Scenario {scn} can not be imported.")

        # Select prognosed population per zensus cell
        zensus = db.select_dataframe(
            f"""SELECT * FROM
            {sources['population_prognosis_zensus']['schema']}.
            {sources['population_prognosis_zensus']['table']}
            WHERE year = {year}
            AND population > 0""", index_col='zensus_population_id')

        # Add nuts3 key to zensus cells
        zensus['nuts3'] = map_nuts3.nuts3

        # Calculate share of nuts3 population per zensus cell
        zensus['population_share'] = zensus.population.groupby(
            zensus.nuts3).apply(lambda grp: grp/grp.sum())

        # Select forecastet electrical demands from demandregio table
        demand_nuts3 = db.select_dataframe(
            f"""SELECT nuts3, SUM(demand) as demand FROM
            {sources['demandregio']['schema']}.
            {sources['demandregio']['table']}
            WHERE scenario = '{scn}'
            GROUP BY nuts3""",
            index_col='nuts3')

        # Scale demands on nuts3 level linear to population share
        zensus['demand'] = zensus['population_share'].mul(
            demand_nuts3.demand[zensus['nuts3']].values)

        # Set scenario name and sector
        zensus['scenario'] = scn
        zensus['sector'] = 'residential'

        # Rename index
        zensus.index = zensus.index.rename('zensus_population_id')

        # Insert data to target table
        zensus[['scenario', 'demand', 'sector']].to_sql(
            target['table'],
            schema=target['schema'],
            con=db.engine(),
            if_exists='append')


def distribute_cts_demands():
    """ Distribute electrical demands for cts to zensus cells.

    The demands on nuts3-level from demandregio are linear distributed
    to the heat demand of cts in each zensus cell.

    Returns
    -------
    None.

    """

    sources = (egon.data.config.datasets()
               ['electrical_demands_cts']['sources'])

    target = (egon.data.config.datasets()
              ['electrical_demands_cts']['targets']
              ['cts_demands_zensus'])

    db.execute_sql(f"""DELETE FROM {target['schema']}.{target['table']}
                   WHERE sector = 'service'""")

    # Select match between zensus cells and nuts3 regions of vg250
    map_nuts3 = db.select_dataframe(
        f"""SELECT zensus_population_id, nuts3 FROM
        {sources['map_zensus_vg250']['schema']}.
        {sources['map_zensus_vg250']['table']}""",
        index_col='zensus_population_id')

    # Insert data per scenario
    for scn in sources['demandregio']['scenarios']:

        # Select heat_demand per zensus cell
        peta = db.select_dataframe(
            f"""SELECT zensus_population_id, demand as heat_demand,
            sector, scenario FROM
            {sources['heat_demand_cts']['schema']}.
            {sources['heat_demand_cts']['table']}
            WHERE scenario = '{scn}'
            AND sector = 'service'""", index_col='zensus_population_id')

        # Add nuts3 key to zensus cells
        peta['nuts3'] = map_nuts3.nuts3

        # Calculate share of nuts3 population per zensus cell
        peta['share'] = peta.heat_demand.groupby(
            peta.nuts3).apply(lambda grp: grp/grp.sum())

        # Select forecastet electrical demands from demandregio table
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
            index_col='nuts3')

        # Scale demands on nuts3 level linear to population share
        peta['demand'] = peta['share'].mul(
            demand_nuts3.demand[peta['nuts3']].values)

        # Rename index
        peta.index = peta.index.rename('zensus_population_id')

        # Insert data to target table
        peta[['scenario', 'demand', 'sector']].to_sql(
            target['table'],
            schema=target['schema'],
            con=db.engine(),
            if_exists='append')


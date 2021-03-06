"""The central module containing all code dealing with scenario table.
"""
from egon.data import db
from sqlalchemy import Column, String, VARCHAR
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
import pandas as pd
import egon.data.importing.scenarios.parameters as parameters

Base = declarative_base()


class EgonScenario(Base):
    __tablename__ = "egon_scenario_parameters"
    __table_args__ = {"schema": "scenario"}
    name = Column(String,  primary_key=True)
    global_parameters = Column(JSONB)
    electricity_parameters = Column(JSONB)
    gas_parameters = Column(JSONB)
    heat_parameters = Column(JSONB)
    mobility_parameters = Column(JSONB)
    description = Column(VARCHAR)


def create_table():
    """Create table for scenarios
    Returns
    -------
    None.
    """
    engine = db.engine()
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS scenario;")
    db.execute_sql(
        "DROP TABLE IF EXISTS scenario.egon_scenario_parameters CASCADE;")
    EgonScenario.__table__.create(bind=engine, checkfirst=True)


def insert_scenarios():
    """Insert scenarios and their parameters to scenario table

    Returns
    -------
    None.

    """

    db.execute_sql("DELETE FROM scenario.egon_scenario_parameters")

    session = sessionmaker(bind=db.engine())()

    # Scenario eGon2035
    egon2035 = EgonScenario(name='eGon2035')

    egon2035.description = """
        The mid-term scenario eGon2035 is based on scenario C 2035 of the
        Netzentwicklungsplan Strom 2035, Version 2021.
        Scenario C 2035 is characretized by an ambitious expansion of
        renewable energies and a higher share of sector coupling.
        Analogous to the Netzentwicklungsplan, the countries bordering germany
        are modeled based on Ten-Year Network Development Plan, Version 2020.
        """
    egon2035.global_parameters = parameters.global_settings(egon2035.name)

    egon2035.electricity_parameters = parameters.electricity(egon2035.name)

    egon2035.gas_parameters = parameters.gas(egon2035.name)

    egon2035.heat_parameters = parameters.heat(egon2035.name)

    egon2035.mobility_parameters = parameters.mobility(egon2035.name)

    session.add(egon2035)

    session.commit()

    # Scenario eGon100RE
    egon100re = EgonScenario(name='eGon100RE')

    egon100re.description = """
        The long-term scenario eGon100RE represents a 100% renewable
        energy secor in Germany.
        """
    egon100re.global_parameters = parameters.global_settings(egon100re.name)

    egon100re.electricity_parameters = parameters.electricity(egon100re.name)

    egon100re.gas_parameters = parameters.gas(egon100re.name)

    egon100re.heat_parameters = parameters.heat(egon100re.name)

    egon100re.mobility_parameters = parameters.mobility(egon100re.name)

    session.add(egon100re)

    session.commit()

def get_sector_parameters(sector, scenario=None):
    """ Returns parameters for each sector as dictionary.

    If scenario=None data for all scenarios is returned as pandas.DataFrame.
    Otherwise the parameters of the specific scenario are returned as a dict.

    Parameters
    ----------
    sector : str
        Name of the sector.
        Options are: ['global', 'electricity', 'heat', 'gas', 'mobility']
    scenario : str, optional
        Name of the scenario. The default is None.

    Returns
    -------
    values : dict or pandas.DataFrane
        List or table of parameters for the selected sector

    """


    if scenario:
        if scenario in db.select_dataframe(
                "SELECT name FROM scenario.egon_scenario_parameters"
                ).name.values:
            values = (
                db.select_dataframe(
                    f"""
                    SELECT {sector}_parameters as val
                    FROM scenario.egon_scenario_parameters
                    WHERE name = '{scenario}';""").val[0]
                )
        else:
            print(f"Scenario name {scenario} is not valid.")
    else:
        values = pd.DataFrame(
            db.select_dataframe(
                    f"""
                    SELECT {sector}_parameters as val
                    FROM scenario.egon_scenario_parameters
                    WHERE name='eGon2035'""",
                    ).val[0],
            index = ['eGon2035']
            ).append(
                pd.DataFrame(
                    db.select_dataframe(
                        f"""
                        SELECT {sector}_parameters as val
                        FROM scenario.egon_scenario_parameters
                        WHERE name='eGon100RE'""",
                        ).val[0],
                    index = ['eGon100RE']
                    )
                )

    return values

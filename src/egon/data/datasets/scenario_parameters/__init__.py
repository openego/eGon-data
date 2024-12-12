"""The central module containing all code dealing with scenario table.
"""
from pathlib import Path
from urllib.request import urlretrieve
import shutil
import zipfile

from sqlalchemy import VARCHAR, Column, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
import egon.data.config
import egon.data.datasets.scenario_parameters.parameters as parameters

Base = declarative_base()


class EgonScenario(Base):
    __tablename__ = "egon_scenario_parameters"
    __table_args__ = {"schema": "scenario"}
    name = Column(String, primary_key=True)
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
        "DROP TABLE IF EXISTS scenario.egon_scenario_parameters CASCADE;"
    )
    EgonScenario.__table__.create(bind=engine, checkfirst=True)


def insert_scenarios():
    """Insert scenarios and their parameters to scenario table

    Returns
    -------
    None.

    """

    db.execute_sql("DELETE FROM scenario.egon_scenario_parameters CASCADE;")

    session = sessionmaker(bind=db.engine())()

    # Scenario eGon2035
    egon2035 = EgonScenario(name="eGon2035")

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
    egon100re = EgonScenario(name="eGon100RE")

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

    # Scenario eGon2021
    eGon2021 = EgonScenario(name="eGon2021")

    eGon2021.description = """
        Status quo scenario for 2021. Note: This is NOT A COMPLETE SCENARIO
        and covers only some sector data required by ding0, such as demand
        on NUTS 3 level and generation units .
        """
    eGon2021.global_parameters = parameters.global_settings(eGon2021.name)

    eGon2021.electricity_parameters = parameters.electricity(eGon2021.name)

    eGon2021.gas_parameters = parameters.gas(eGon2021.name)

    eGon2021.heat_parameters = parameters.heat(eGon2021.name)

    eGon2021.mobility_parameters = parameters.mobility(eGon2021.name)

    session.add(eGon2021)
    
    session.commit()    
    
    # Scenario status2019
    status2019 = EgonScenario(name="status2019")

    status2019.description = """
        Status quo ante scenario for 2019 for validation use within the project PoWerD.
        """
    status2019.global_parameters = parameters.global_settings(status2019.name)

    status2019.electricity_parameters = parameters.electricity(status2019.name)

    status2019.gas_parameters = parameters.gas(status2019.name)

    status2019.heat_parameters = parameters.heat(status2019.name)

    status2019.mobility_parameters = parameters.mobility(status2019.name)

    session.add(status2019)

    session.commit()


def get_sector_parameters(sector, scenario=None):
    """Returns parameters for each sector as dictionary.

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
        if (
            scenario
            in db.select_dataframe(
                "SELECT name FROM scenario.egon_scenario_parameters"
            ).name.values
        ):
            values = db.select_dataframe(
                f"""
                    SELECT {sector}_parameters as val
                    FROM scenario.egon_scenario_parameters
                    WHERE name = '{scenario}';"""
            ).val[0]
        else:
            print(f"Scenario name {scenario} is not valid.")
    else:
        values = pd.concat([
            pd.DataFrame(
            db.select_dataframe(
                f"""
                    SELECT {sector}_parameters as val
                    FROM scenario.egon_scenario_parameters
                    WHERE name='eGon2035'"""
            ).val[0],
            index=["eGon2035"]),
            pd.DataFrame(
                db.select_dataframe(
                    f"""
                        SELECT {sector}_parameters as val
                        FROM scenario.egon_scenario_parameters
                        WHERE name='eGon100RE'"""
                ).val[0],
                index=["eGon100RE"],
            ),
        
            pd.DataFrame(
                db.select_dataframe(
                    f"""
                        SELECT {sector}_parameters as val
                        FROM scenario.egon_scenario_parameters
                        WHERE name='eGon2021'"""
                ).val[0],
                index=["eGon2021"],
            )
            ], ignore_index=True)
        

    return values


def download_pypsa_technology_data():
    """Downlad PyPSA technology data results."""
    data_path = Path(".") / "pypsa_technology_data"
    # Delete folder if it already exists
    if data_path.exists() and data_path.is_dir():
        shutil.rmtree(data_path)
    # Get parameters from config and set download URL
    sources = egon.data.config.datasets()["pypsa-technology-data"]["sources"][
        "zenodo"
    ]
    url = f"""https://zenodo.org/record/{sources['deposit_id']}/files/{sources['file']}"""
    target_file = egon.data.config.datasets()["pypsa-technology-data"][
        "targets"
    ]["file"]

    # Retrieve files
    urlretrieve(url, target_file)

    with zipfile.ZipFile(target_file, "r") as zip_ref:
        zip_ref.extractall(".")


class ScenarioParameters(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="ScenarioParameters",

            version="0.0.17",

            dependencies=dependencies,
            tasks=(
                create_table,
                download_pypsa_technology_data,
                insert_scenarios,
            ),
        )
